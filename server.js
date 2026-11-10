const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { MongoClient } = require('mongodb');

const app = express();
const srv = http.createServer(app);
const io = new Server(srv, {
  cors: { origin: true, credentials: true }
});

app.use(express.static('public'));

app.get('/health', (_req, res) =>
  res.json({ ok: true, service: 'RS LUDO' })
);

const rooms = Object.create(null);
const parties = Object.create(null);
const gameRooms = Object.create(null);
const online = new Map();

const SEATS = [0, 2, 1, 3];
const SAFE = [0, 8, 13, 21, 26, 34, 39, 47];
const FEES = [0, 50, 100, 200, 500];
const RP = [0, 100, 300, 500, 800, 1200, 2000, 5000];

const mem = new Map();
let col = null;
let mongoClient = null;

const mongoUri =
  process.env.MONGO_URL || process.env.MONGODB_URI;

const dbName = process.env.MONGO_DB_NAME || 'rsludo';

const dbReady = mongoUri
  ? (async () => {
      try {
        mongoClient = new MongoClient(mongoUri, {
          serverSelectionTimeoutMS: 10000
        });

        await mongoClient.connect();

        col = mongoClient.db(dbName).collection('users');

        await col.createIndex({ name: 1 });

        console.log(`MongoDB connected: ${dbName}.users`);
      } catch (err) {
        console.error('MongoDB connection failed:', err.message);
        throw err;
      }
    })()
  : Promise.resolve();

async function getU(id) {
  await dbReady;
  return col
    ? col.findOne({ _id: String(id) })
    : mem.get(String(id));
}

async function putU(user) {
  await dbReady;

  if (col) {
    await col.replaceOne(
      { _id: user._id },
      user,
      { upsert: true }
    );
  } else {
    mem.set(user._id, user);
  }
}

function pu(u) {
  return {
    id: u._id,
    name: u.name,
    pic: u.pic || '',
    ring: u.ring || 0,
    rings: u.rings || [0],
    coins: u.coins
  };
}

function pi(u) {
  return {
    id: u._id,
    name: u.name,
    pic: u.pic || '',
    ring: u.ring || 0
  };
}

function ab(seat, p) {
  return (seat * 13 + p) % 52;
}

function legal(pl, dice) {
  return pl.pieces
    .map((_, i) => i)
    .filter(i =>
      pl.pieces[i] < 0
        ? dice === 6
        : pl.pieces[i] + dice <= 56
    );
}

function next(r) {
  let n = 0;

  do {
    r.turn = (r.turn + 1) % r.players.length;
    n++;
  } while (r.players[r.turn].left && n < 9);

  r.phase = 'roll';
  r.dice = 0;
}

function safeEmit(room, event, data) {
  io.to(room).emit(event, data);
}

function makeCode(len = 5) {
  return Math.random()
    .toString(36)
    .slice(2, 2 + len)
    .toUpperCase();
}

function pubP(p) {
  safeEmit('p' + p.code, 'party', {
    code: p.code,
    host: p.host,
    fee: p.fee,
    m: p.m
  });
}

async function sendF(uid) {
  const sid = online.get(uid);
  if (!sid) return;

  const u = await getU(uid);
  if (!u) return;

  const list = async arr => {
    const out = [];

    for (const id of arr || []) {
      const friend = await getU(id);

      if (friend) {
        out.push({
          ...pi(friend),
          online: online.has(id)
        });
      }
    }

    return out;
  };

  io.to(sid).emit('friends', {
    friends: await list(u.friends),
    reqs: await list(u.reqs)
  });
}

async function presence(u) {
  for (const fid of u.friends || []) {
    await sendF(fid);
  }
}

/* =================================
   NEW MULTIPLAYER GAMES
================================= */

const GAME_RULES = {
  carrom: { min: 2, max: 4 },
  racing: { min: 2, max: 6 },
  battle: { min: 2, max: 16 }
};

function publicGameRoom(room) {
  return {
    code: room.code,
    game: room.game,
    host: room.host,
    status: room.status,
    max: room.max,

    players: room.players.map(p => ({
      id: p.uid,
      name: p.name,
      pic: p.pic || '',
      ready: !!p.ready,
      alive: p.alive !== false,
      score: p.score || 0,
      lap: p.lap || 0,
      health: p.health ?? 100
    })),

    map: room.map,
    createdAt: room.createdAt
  };
}

function emitGameRoom(room) {
  safeEmit(
    'g' + room.code,
    'game:room',
    publicGameRoom(room)
  );
}

function leaveGameRoom(socket) {
  const code = socket.data.gameRoom;

  if (!code || !gameRooms[code]) {
    delete socket.data.gameRoom;
    return;
  }

  const room = gameRooms[code];

  room.players = room.players.filter(
    p => p.sid !== socket.id
  );

  socket.leave('g' + code);
  delete socket.data.gameRoom;

  if (!room.players.length) {
    delete gameRooms[code];
  } else {
    if (!room.players.some(p => p.uid === room.host)) {
      room.host = room.players[0].uid;
    }

    emitGameRoom(room);
  }
}

function createGameRoom(game, user, options = {}) {
  const rules = GAME_RULES[game];
  const code = makeCode(6);

  const room = gameRooms[code] = {
    code,
    game,
    host: user._id,
    status: 'lobby',

    max: Math.max(
      rules.min,
      Math.min(
        rules.max,
        Number(options.max) || rules.max
      )
    ),

    map:
      typeof options.map === 'string'
        ? options.map.slice(0, 40)
        : 'default',

    createdAt: Date.now(),
    turn: 0,
    state: {},
    players: []
  };

  return room;
}

/* =================================
   SOCKET CONNECTION
================================= */

io.on('connection', socket => {

  const R = () => rooms[socket.data.code];

  const pub = room =>
    safeEmit(room.code, 'state', room);

  const U = async () =>
    socket.data.uid
      ? getU(socket.data.uid)
      : null;

  const sendMe = async () => {
    const u = await U();
    if (u) socket.emit('me', pu(u));
  };

  const requireUser = async () => {
    const u = await U();

    if (!u) {
      socket.emit('err', 'আগে অ্যাকাউন্টে লগইন করুন');
    }

    return u;
  };

  /* =================================
     PARTY SYSTEM
  ================================= */

  const leaveP = () => {
    const code = socket.data.pcode;
    const p = parties[code];

    if (!p) return;

    p.m = p.m.filter(
      x => x.sid !== socket.id
    );

    socket.leave('p' + code);

    delete socket.data.pcode;

    socket.emit('party', null);

    if (!p.m.length) {
      delete parties[code];
    } else {
      if (!p.m.some(x => x.uid === p.host)) {
        p.host = p.m[0].uid;
      }

      pubP(p);
    }
  };

  const joinP = (code, user) => {
    const p = parties[code];

    if (!p) {
      return socket.emit(
        'err',
        'পার্টি পাওয়া যায়নি'
      );
    }

    if (socket.data.pcode === code) return;

    if (p.m.length >= 4) {
      return socket.emit('err', 'পার্টি ভর্তি');
    }

    leaveP();

    p.m.push({
      sid: socket.id,
      uid: user._id,
      ...pi(user)
    });

    socket.join('p' + code);
    socket.data.pcode = code;

    pubP(p);
  };

  const mkP = user => {
    const code = makeCode(4);

    parties[code] = {
      code,
      host: user._id,
      fee: 0,
      m: []
    };

    joinP(code, user);
  };

  const upd = user => {
    const p = parties[socket.data.pcode];

    const member = p &&
      p.m.find(x => x.uid === user._id);

    if (member) {
      Object.assign(member, pi(user));
      pubP(p);
    }
  };

  const enter = async user => {
    socket.data.uid = user._id;

    online.set(user._id, socket.id);

    socket.emit('me', pu(user));

    if (!socket.data.pcode) {
      mkP(user);
    }

    await sendF(user._id);
    await presence(user);
  };

  /* =================================
     ACCOUNT
  ================================= */

  socket.on('register', async rawName => {
    try {
      await dbReady;

      const name = String(rawName || '')
        .replace(/[<>&"'`]/g, '')
        .trim()
        .slice(0, 12);

      if (!name) {
        return socket.emit('err', 'নাম দিন');
      }

      let id;

      do {
        id = String(
          100000 +
          Math.floor(Math.random() * 900000)
        );
      } while (await getU(id));

      const user = {
        _id: id,
        name,
        pic: '',
        coins: 1000,
        bonus: 0,
        ring: 0,
        rings: [0],
        friends: [],
        reqs: [],
        createdAt: Date.now()
      };

      await putU(user);
      await enter(user);

    } catch (err) {
      console.error('register:', err.message);

      socket.emit(
        'err',
        'অ্যাকাউন্ট তৈরি করা যায়নি। সার্ভার লগ দেখুন।'
      );
    }
  });

  socket.on('login', async rawId => {
    try {
      await dbReady;

      const user = await getU(
        String(rawId || '').trim()
      );

      if (!user) {
        return socket.emit('badid');
      }

      await enter(user);

    } catch (err) {
      console.error('login:', err.message);
      socket.emit('err', 'লগইন করা যায়নি');
    }
  });

  socket.on('pic', async data => {
    const user = await requireUser();

    if (
      !user ||
      typeof data !== 'string' ||
      data.length > 30000 ||
      !/^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(data)
    ) return;

    user.pic = data;

    await putU(user);
    await sendMe();
    upd(user);
  });

  socket.on('bonus', async () => {
    const user = await requireUser();
    if (!user) return;

    if (user.coins >= 100) {
      return socket.emit(
        'err',
        'কয়েন ১০০ এর কম হলে বোনাস পাবেন'
      );
    }

    if (Date.now() - (user.bonus || 0) < 86400000) {
      return socket.emit(
        'err',
        'বোনাস ২৪ ঘণ্টায় একবার'
      );
    }

    user.coins += 200;
    user.bonus = Date.now();

    await putU(user);
    await sendMe();

    socket.emit('err', '🎁 ২০০ কয়েন পেয়েছেন!');
  });

  socket.on('buy', async rawIndex => {
    const user = await requireUser();
    const index = Number(rawIndex);

    if (
      !user ||
      !Number.isInteger(index) ||
      index < 1 ||
      index >= RP.length
    ) return;

    user.rings = user.rings || [0];

    if (user.rings.includes(index)) return;

    if (user.coins < RP[index]) {
      return socket.emit('err', 'কয়েন কম');
    }

    user.coins -= RP[index];
    user.rings.push(index);
    user.ring = index;

    await putU(user);
    await sendMe();
    upd(user);

    socket.emit('err', '✅ রিং কেনা হয়েছে!');
  });

  socket.on('setring', async rawIndex => {
    const user = await requireUser();
    const index = Number(rawIndex);

    if (
      !user ||
      !(user.rings || [0]).includes(index)
    ) return;

    user.ring = index;

    await putU(user);
    await sendMe();
    upd(user);
  });

  /* =================================
     FRIENDS
  ================================= */

  socket.on('addf', async rawId => {
    const user = await requireUser();
    if (!user) return;

    const id = String(rawId || '').trim();
    const target = await getU(id);

    if (!target) {
      return socket.emit(
        'err',
        'এই আইডি পাওয়া যায়নি'
      );
    }

    if (target._id === user._id) {
      return socket.emit(
        'err',
        'নিজেকে অ্যাড করা যায় না'
      );
    }

    target.reqs = target.reqs || [];
    user.friends = user.friends || [];

    if (user.friends.includes(target._id)) {
      return socket.emit('err', 'আগেই বন্ধু');
    }

    if (target.reqs.includes(user._id)) {
      return socket.emit(
        'err',
        'রিকোয়েস্ট আগেই পাঠানো'
      );
    }

    target.reqs.push(user._id);

    await putU(target);
    await sendF(target._id);

    socket.emit(
      'err',
      '📨 রিকোয়েস্ট পাঠানো হয়েছে'
    );
  });

  socket.on('accept', async rawId => {
    const user = await requireUser();
    const target = await getU(String(rawId));

    if (
      !user ||
      !target ||
      !(user.reqs || []).includes(target._id)
    ) return;

    user.reqs = user.reqs.filter(
      x => x !== target._id
    );

    user.friends = user.friends || [];
    target.friends = target.friends || [];

    if (!user.friends.includes(target._id)) {
      user.friends.push(target._id);
    }

    if (!target.friends.includes(user._id)) {
      target.friends.push(user._id);
    }

    await putU(user);
    await putU(target);

    await sendF(user._id);
    await sendF(target._id);
  });

  socket.on('decl', async rawId => {
    const user = await requireUser();
    if (!user) return;

    user.reqs = (user.reqs || []).filter(
      x => x !== String(rawId)
    );

    await putU(user);
    await sendF(user._id);
  });

  socket.on('invite', async rawFriendId => {
    const user = await requireUser();

    const friendId = String(rawFriendId);
    const sid = online.get(friendId);

    if (
      !user ||
      !sid ||
      !(user.friends || []).includes(friendId)
    ) return;

    if (!socket.data.pcode) {
      mkP(user);
    }

    io.to(sid).emit('invite', {
      from: user.name,
      code: socket.data.pcode
    });

    socket.emit(
      'err',
      '📨 ইনভাইট পাঠানো হয়েছে'
    );
  });

  socket.on('acceptinv', async code => {
    const user = await requireUser();

    if (user) {
      joinP(String(code).toUpperCase(), user);
    }
  });

  socket.on('joinparty', async code => {
    const user = await requireUser();

    if (user) {
      joinP(String(code).toUpperCase(), user);
    }
  });

  socket.on('pfee', fee => {
    const p = parties[socket.data.pcode];

    if (
      !p ||
      !FEES.includes(Number(fee)) ||
      p.host !== socket.data.uid
    ) return;

    p.fee = Number(fee);
    pubP(p);
  });

  /* =================================
     LUDO START
  ================================= */

  socket.on('pstart', async () => {
    const p = parties[socket.data.pcode];

    if (
      !p ||
      p.host !== socket.data.uid ||
      socket.data.code
    ) return;

    if (p.m.length < 2) {
      return socket.emit(
        'err',
        'কমপক্ষে ২ জন লাগবে'
      );
    }

    const users = [];

    for (const m of p.m) {
      const user = await getU(m.uid);

      if (!user || user.coins < p.fee) {
        return socket.emit(
          'err',
          m.name + ' এর কয়েন কম'
        );
      }

      users.push(user);
    }

    const code = makeCode(4);

    const room = rooms[code] = {
      code,
      fee: p.fee,
      pot: p.fee * users.length,
      players: [],
      turn: 0,
      dice: 0,
      phase: 'roll',
      msg: '',
      winner: null
    };

    for (let k = 0; k < users.length; k++) {
      const user = users[k];
      const member = p.m[k];

      user.coins -= p.fee;

      await putU(user);

      room.players.push({
        id: member.sid,
        uid: user._id,
        ...pi(user),
        coins: user.coins,
        seat: SEATS[k],
        pieces: [-1, -1, -1, -1]
      });

      const playerSocket =
        io.sockets.sockets.get(member.sid);

      if (playerSocket) {
        playerSocket.join(code);
        playerSocket.data.code = code;
        playerSocket.emit('me', pu(user));
      }
    }

    room.msg =
      'খেলা শুরু! ' +
      room.players[0].name +
      ' আগে ফেলুক';

    pub(room);
  });

  /* =================================
     VOICE CHAT SIGNALING
  ================================= */

  socket.on('sig', (to, data) => {
    if (
      typeof to !== 'string' ||
      !data ||
      typeof data !== 'object'
    ) return;

    const target =
      io.sockets.sockets.get(to);

    if (!target) return;

    const sameParty =
      socket.data.pcode &&
      socket.data.pcode === target.data.pcode;

    const sameLudo =
      socket.data.code &&
      socket.data.code === target.data.code;

    const sameGame =
      socket.data.gameRoom &&
      socket.data.gameRoom === target.data.gameRoom;

    if (sameParty || sameLudo || sameGame) {
      target.emit('sig', socket.id, data);
    }
  });

  /* =================================
     LUDO DICE
  ================================= */

  socket.on('roll', () => {
    const room = R();

    if (
      !room ||
      room.phase !== 'roll' ||
      !room.players[room.turn] ||
      room.players[room.turn].id !== socket.id
    ) return;

    const player = room.players[room.turn];

    room.phase = 'rolling';
    room.msg = player.name + ' ডাইস ফেলছে...';

    pub(room);

    setTimeout(() => {
      if (
        !rooms[room.code] ||
        room.phase !== 'rolling'
      ) return;

      const dice =
        1 + Math.floor(Math.random() * 6);

      room.dice = dice;

      if (legal(player, dice).length) {
        room.phase = 'move';

        room.msg =
          player.name +
          ' ' +
          dice +
          ' ফেলল' +
          (dice === 6 ? ' 🔥 ছক্কা!' : '');

      } else {
        room.phase = 'wait';

        room.msg =
          player.name +
          ' ' +
          dice +
          ' ফেলল, চাল নেই 😅';

        setTimeout(() => {
          if (
            rooms[room.code] &&
            room.phase === 'wait'
          ) {
            next(room);

            room.msg =
              room.players[room.turn].name +
              ' এর পালা';

            pub(room);
          }
        }, 1500);
      }

      pub(room);
    }, 800);
  });

  async function payout(room, player) {
    if (!room.pot) return;

    const user = await getU(player.uid);
    if (!user) return;

    user.coins += room.pot;

    await putU(user);

    player.coins = user.coins;

    room.msg =
      '🏆 ' +
      player.name +
      ' জিতেছে! 🪙+' +
      room.pot;

    io.to(player.id).emit('me', pu(user));

    pub(room);
  }

  socket.on('move', rawIndex => {
    const room = R();
    const index = Number(rawIndex);

    if (
      !room ||
      room.phase !== 'move' ||
      !room.players[room.turn] ||
      room.players[room.turn].id !== socket.id
    ) return;

    const player = room.players[room.turn];
    const dice = room.dice;

    if (
      !Number.isInteger(index) ||
      !legal(player, dice).includes(index)
    ) return;

    const position =
      player.pieces[index] < 0
        ? 0
        : player.pieces[index] + dice;

    player.pieces[index] = position;

    let captured = null;

    if (
      position <= 50 &&
      !SAFE.includes(ab(player.seat, position))
    ) {
      room.players.forEach(other => {
        if (other !== player) {
          other.pieces.forEach((q, k) => {
            if (
              q >= 0 &&
              q <= 50 &&
              ab(other.seat, q) ===
                ab(player.seat, position)
            ) {
              other.pieces[k] = -1;
              captured = other.name;
            }
          });
        }
      });
    }

    if (player.pieces.every(p => p === 56)) {
      room.phase = 'over';
      room.winner = player.name;

      room.msg =
        '🏆 ' + player.name + ' জিতেছে!';

      pub(room);
      payout(room, player);

      return;
    }

    if (dice === 6 || captured) {
      room.phase = 'roll';
      room.dice = 0;

      room.msg = captured
        ? '😂 ' +
          captured +
          ' এর গুটি কাটা পড়ল! ' +
          player.name +
          ' আবার ফেলবে'
        : player.name + ' আবার ফেলবে';

    } else {
      next(room);

      room.msg =
        room.players[room.turn].name +
        ' এর পালা';
    }

    pub(room);
  });

  const leaveLudo = () => {
    const room = R();

    if (!room) return;

    const index = room.players.findIndex(
      p => p.id === socket.id
    );

    socket.leave(room.code);
    delete socket.data.code;

    if (index < 0) return;

    room.players[index].left = true;

    if (room.players.every(p => p.left)) {
      delete rooms[room.code];
      return;
    }

    if (
      room.phase !== 'over' &&
      room.turn === index
    ) {
      next(room);
    }

    room.msg = 'একজন বের হয়ে গেছে';

    pub(room);
  };

  socket.on('lgame', leaveLudo);

  /* =================================
     CREATE NEW GAME ROOM
     carrom / racing / battle
  ================================= */

  socket.on('game:create', async (game, options = {}) => {
    const user = await requireUser();

    if (!user) return;

    if (
      !Object.prototype.hasOwnProperty.call(
        GAME_RULES,
        game
      )
    ) {
      return socket.emit(
        'err',
        'এই গেমটি পাওয়া যায়নি'
      );
    }

    leaveGameRoom(socket);

    const room = createGameRoom(
      game,
      user,
      options
    );

    room.players.push({
      sid: socket.id,
      uid: user._id,
      name: user.name,
      pic: user.pic || '',
      ready: false,
      alive: true,
      score: 0,
      lap: 0,
      health: 100,
      x: 0,
      y: 0,
      angle: 0
    });

    socket.join('g' + room.code);
    socket.data.gameRoom = room.code;

    socket.emit(
      'game:created',
      publicGameRoom(room)
    );

    emitGameRoom(room);
  });

  /* =================================
     JOIN GAME ROOM
  ================================= */

  socket.on('game:join', async rawCode => {
    const user = await requireUser();

    if (!user) return;

    const code = String(rawCode || '')
      .trim()
      .toUpperCase();

    const room = gameRooms[code];

    if (!room) {
      return socket.emit(
        'err',
        'গেম রুম পাওয়া যায়নি'
      );
    }

    if (room.status !== 'lobby') {
      return socket.emit(
        'err',
        'গেম ইতিমধ্যে শুরু হয়েছে'
      );
    }

    if (room.players.length >= room.max) {
      return socket.emit('err', 'রুম ভর্তি');
    }

    if (
      room.players.some(p => p.uid === user._id)
    ) {
      return socket.emit(
        'err',
        'তুমি ইতিমধ্যে এই রুমে আছ'
      );
    }

    leaveGameRoom(socket);

    room.players.push({
      sid: socket.id,
      uid: user._id,
      name: user.name,
      pic: user.pic || '',
      ready: false,
      alive: true,
      score: 0,
      lap: 0,
      health: 100,
      x: 0,
      y: 0,
      angle: 0
    });

    socket.join('g' + code);
    socket.data.gameRoom = code;

    socket.emit(
      'game:joined',
      publicGameRoom(room)
    );

    emitGameRoom(room);
  });

  socket.on('game:leave', () =>
    leaveGameRoom(socket)
  );

  /* =================================
     PLAYER READY
  ================================= */

  socket.on('game:ready', ready => {
    const room =
      gameRooms[socket.data.gameRoom];

    if (
      !room ||
      room.status !== 'lobby'
    ) return;

    const player = room.players.find(
      p => p.sid === socket.id
    );

    if (!player) return;

    player.ready = !!ready;

    emitGameRoom(room);
  });

  /* =================================
     START GAME
  ================================= */

  socket.on('game:start', () => {
    const room =
      gameRooms[socket.data.gameRoom];

    if (
      !room ||
      room.host !== socket.data.uid ||
      room.status !== 'lobby'
    ) return;

    const rules = GAME_RULES[room.game];

    if (room.players.length < rules.min) {
      return socket.emit(
        'err',
        `কমপক্ষে ${rules.min} জন খেলোয়াড় লাগবে`
      );
    }

    room.status = 'playing';
    room.startedAt = Date.now();

    room.players.forEach(p => {
      p.alive = true;
      p.health = 100;
    });

    emitGameRoom(room);

    safeEmit(
      'g' + room.code,
      'game:started',
      {
        game: room.game,
        code: room.code,
        map: room.map,
        startedAt: room.startedAt
      }
    );
  });

  /* =================================
     GAME ACTIONS
  ================================= */

  socket.on('game:action', payload => {
    const room =
      gameRooms[socket.data.gameRoom];

    if (
      !room ||
      room.status !== 'playing' ||
      !payload ||
      typeof payload !== 'object'
    ) return;

    const player = room.players.find(
      p => p.sid === socket.id
    );

    if (
      !player ||
      player.alive === false
    ) return;

    const raw = JSON.stringify(payload);

    if (raw.length > 1500) return;

    const action = {
      uid: player.uid,
      name: player.name,
      action: payload.action,
      data: payload.data
    };

    if (
      room.game === 'racing' &&
      payload.action === 'race:update'
    ) {
      const d = payload.data || {};

      player.x = Number.isFinite(d.x)
        ? Math.max(-10000, Math.min(10000, d.x))
        : player.x;

      player.y = Number.isFinite(d.y)
        ? Math.max(-10000, Math.min(10000, d.y))
        : player.y;

      player.angle = Number.isFinite(d.angle)
        ? d.angle
        : player.angle;

      player.lap = Number.isFinite(d.lap)
        ? Math.max(0, Math.min(99, Math.floor(d.lap)))
        : player.lap;

      player.score = Number.isFinite(d.score)
        ? Math.max(0, Math.min(1000000, d.score))
        : player.score;

      safeEmit(
        'g' + room.code,
        'game:player-update',
        {
          id: player.uid,
          x: player.x,
          y: player.y,
          angle: player.angle,
          lap: player.lap,
          score: player.score
        }
      );

    } else if (
      room.game === 'battle' &&
      payload.action === 'battle:update'
    ) {
      const d = payload.data || {};

      player.x = Number.isFinite(d.x)
        ? Math.max(-10000, Math.min(10000, d.x))
        : player.x;

      player.y = Number.isFinite(d.y)
        ? Math.max(-10000, Math.min(10000, d.y))
        : player.y;

      player.angle = Number.isFinite(d.angle)
        ? d.angle
        : player.angle;

      safeEmit(
        'g' + room.code,
        'game:player-update',
        {
          id: player.uid,
          x: player.x,
          y: player.y,
          angle: player.angle
        }
      );

    } else {
      safeEmit(
        'g' + room.code,
        'game:action',
        action
      );
    }
  });

  /* =================================
     GAME FINISH
  ================================= */

  socket.on('game:finish', result => {
    const room =
      gameRooms[socket.data.gameRoom];

    if (
      !room ||
      room.status !== 'playing'
    ) return;

    const player = room.players.find(
      p => p.sid === socket.id
    );

    if (!player) return;

    if (room.game === 'racing') {
      player.score = Math.max(
        player.score || 0,
        Number(result && result.score) || 0
      );
    }

    player.finished = true;

    if (
      room.players.every(p => p.finished)
    ) {
      room.status = 'finished';

      safeEmit(
        'g' + room.code,
        'game:finished',
        publicGameRoom(room)
      );
    }

    emitGameRoom(room);
  });

  /* =================================
     BATTLE ELIMINATION REQUEST
  ================================= */

  socket.on('game:eliminate', uid => {
    const room =
      gameRooms[socket.data.gameRoom];

    if (
      !room ||
      room.game !== 'battle' ||
      room.status !== 'playing'
    ) return;

    const attacker = room.players.find(
      p => p.sid === socket.id
    );

    const victim = room.players.find(
      p => p.uid === String(uid)
    );

    if (
      !attacker ||
      !victim ||
      attacker.uid === victim.uid ||
      victim.alive === false
    ) return;

    safeEmit(
      'g' + room.code,
      'game:combat-request',
      {
        from: attacker.uid,
        to: victim.uid
      }
    );
  });

  /* =================================
     DISCONNECT
  ================================= */

  socket.on('disconnect', async () => {
    leaveLudo();
    leaveP();
    leaveGameRoom(socket);

    const id = socket.data.uid;

    if (
      id &&
      online.get(id) === socket.id
    ) {
      online.delete(id);

      const user = await getU(id);

      if (user) {
        await presence(user);
      }
    }
  });

});

/* =================================
   SERVER START
================================= */

const port = Number(process.env.PORT) || 3000;

srv.listen(port, () => {
  console.log(
    `RS LUDO server listening on ${port}`
  );
});

process.on('SIGTERM', async () => {
  try {
    if (mongoClient) {
      await mongoClient.close();
    }
  } finally {
    process.exit(0);
  }
});
