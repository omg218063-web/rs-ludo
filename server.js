
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { MongoClient } = require('mongodb');

const app = express();
const srv = http.createServer(app);

const io = new Server(srv, {
  cors: {
    origin: true,
    credentials: true
  }
});

app.use(express.json({ limit: '1mb' }));
app.use(express.static('public'));

const PORT = Number(process.env.PORT) || 3000;

const MONGO_URI =
  process.env.MONGODB_URI || process.env.MONGO_URL;

const DB_NAME =
  process.env.MONGO_DB_NAME || 'rswallet';

let mongoClient = null;
let db = null;
let users = null;
let dbConnected = false;

/* =================================
   MONGODB CONNECTION
================================= */

const dbReady = (async () => {
  try {
    if (!MONGO_URI) {
      throw new Error('MongoDB URL is missing');
    }

    mongoClient = new MongoClient(MONGO_URI, {
      serverSelectionTimeoutMS: 10000,
      connectTimeoutMS: 10000
    });

    await mongoClient.connect();

    db = mongoClient.db(DB_NAME);
    users = db.collection('users');

    await users.createIndex({ name: 1 });

    dbConnected = true;

    console.log('MongoDB connected successfully');
    console.log('Database:', DB_NAME);

    return true;

  } catch (error) {
    dbConnected = false;
    console.error('MongoDB connection failed:', error.message);

    if (mongoClient) {
      await mongoClient.close().catch(() => {});
      mongoClient = null;
    }

    return false;
  }
})();

async function getU(id) {
  await dbReady;

  if (!dbConnected || !users) {
    throw new Error('MongoDB is not connected');
  }

  return users.findOne({
    _id: String(id)
  });
}

async function putU(user) {
  await dbReady;

  if (!dbConnected || !users) {
    throw new Error('MongoDB is not connected');
  }

  await users.replaceOne(
    { _id: user._id },
    user,
    { upsert: true }
  );
}

/* =================================
   HEALTH CHECK
================================= */

app.get('/health', (_req, res) => {
  res.status(200).json({
    ok: true,
    service: 'RS LUDO',
    server: 'online',
    mongodb: dbConnected ? 'connected' : 'disconnected'
  });
});

/* =================================
   GAME DATA
================================= */

const rooms = Object.create(null);
const parties = Object.create(null);
const gameRooms = Object.create(null);

const online = new Map();

const SEATS = [0, 2, 1, 3];

const SAFE = [0, 8, 13, 21, 26, 34, 39, 47];

const FEES = [0, 50, 100, 200, 500];

const RP = [0, 100, 300, 500, 800, 1200, 2000, 5000];

const GAME_RULES = {
  carrom: { min: 2, max: 4 },
  racing: { min: 2, max: 6 },
  battle: { min: 2, max: 16 }
};

/* =================================
   COMMON FUNCTIONS
================================= */

function makeCode(len = 5) {
  return Math.random()
    .toString(36)
    .slice(2, 2 + len)
    .toUpperCase();
}

function emitRoom(room, event, data) {
  io.to(room).emit(event, data);
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

function legal(player, dice) {
  return player.pieces
    .map((_, i) => i)
    .filter(i =>
      player.pieces[i] < 0
        ? dice === 6
        : player.pieces[i] + dice <= 56
    );
}

function next(room) {
  let count = 0;

  do {
    room.turn = (room.turn + 1) % room.players.length;
    count++;
  } while (
    room.players[room.turn].left &&
    count < 9
  );

  room.phase = 'roll';
  room.dice = 0;
}

/* =================================
   FRIEND SYSTEM
================================= */

async function sendF(uid) {
  const sid = online.get(uid);
  if (!sid) return;

  const user = await getU(uid);
  if (!user) return;

  async function list(ids) {
    const result = [];

    for (const id of ids || []) {
      const friend = await getU(id);

      if (friend) {
        result.push({
          ...pi(friend),
          online: online.has(id)
        });
      }
    }

    return result;
  }

  io.to(sid).emit('friends', {
    friends: await list(user.friends),
    reqs: await list(user.reqs)
  });
}

async function presence(user) {
  for (const id of user.friends || []) {
    await sendF(id);
  }
}

/* =================================
   PARTY SYSTEM
================================= */

function pubP(party) {
  emitRoom('p' + party.code, 'party', {
    code: party.code,
    host: party.host,
    fee: party.fee,
    m: party.m
  });
}

/* =================================
   OTHER GAME ROOMS
================================= */

function publicGameRoom(room) {
  return {
    code: room.code,
    game: room.game,
    host: room.host,
    status: room.status,
    max: room.max,
    map: room.map,
    createdAt: room.createdAt,

    players: room.players.map(p => ({
      id: p.uid,
      name: p.name,
      pic: p.pic || '',
      ready: !!p.ready,
      alive: p.alive !== false,
      score: p.score || 0,
      lap: p.lap || 0,
      health: p.health ?? 100
    }))
  };
}

function emitGameRoom(room) {
  emitRoom(
    'g' + room.code,
    'game:room',
    publicGameRoom(room)
  );
}

function createGameRoom(game, user, options = {}) {
  const rules = GAME_RULES[game];
  const code = makeCode(6);

  return gameRooms[code] = {
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

    map: typeof options.map === 'string'
      ? options.map.slice(0, 40)
      : 'default',

    createdAt: Date.now(),
    players: [],
    state: {}
  };
}

/* =================================
   SOCKET CONNECTION
================================= */

io.on('connection', socket => {

  console.log('Player connected:', socket.id);

  const on = (event, handler) => {
    socket.on(event, (...args) => {
      Promise.resolve()
        .then(() => handler(...args))
        .catch(error => {
          console.error(
            `Socket event ${event}:`,
            error.message
          );

          socket.emit(
            'err',
            'সার্ভারে সমস্যা হয়েছে। আবার চেষ্টা করুন।'
          );
        });
    });
  };

  const U = async () => {
    return socket.data.uid
      ? getU(socket.data.uid)
      : null;
  };

  const requireUser = async () => {
    const user = await U();

    if (!user) {
      socket.emit('err', 'আগে লগইন করুন');
    }

    return user;
  };

  const sendMe = async () => {
    const user = await U();

    if (user) {
      socket.emit('me', pu(user));
    }
  };

  const pub = room => {
    emitRoom(room.code, 'state', room);
  };

  /* =================================
     LOGIN / REGISTRATION
  ================================= */

  async function enter(user) {
    socket.data.uid = user._id;

    online.set(user._id, socket.id);

    socket.emit('me', pu(user));

    if (!socket.data.pcode) {
      makeParty(user);
    }

    await sendF(user._id);
    await presence(user);
  }

  on('register', async rawName => {
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
        100000 + Math.floor(Math.random() * 900000)
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
  });

  on('login', async rawId => {
    const id = String(rawId || '').trim();

    if (!/^\d{6}$/.test(id)) {
      return socket.emit('badid');
    }

    const user = await getU(id);

    if (!user) {
      return socket.emit('badid');
    }

    await enter(user);
  });

  /* =================================
     PROFILE
  ================================= */

  on('pic', async data => {
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
  });

  on('bonus', async () => {
    const user = await requireUser();
    if (!user) return;

    if (user.coins >= 100) {
      return socket.emit('err', 'কয়েন ১০০ এর কম হলে বোনাস পাবেন');
    }

    if (
      Date.now() - (user.bonus || 0) <
      86400000
    ) {
      return socket.emit('err', '২৪ ঘণ্টায় একবার বোনাস পাবেন');
    }

    user.coins += 200;
    user.bonus = Date.now();

    await putU(user);
    await sendMe();

    socket.emit('err', '🎁 ২০০ কয়েন পেয়েছেন!');
  });

  on('buy', async rawIndex => {
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
      return socket.emit('err', 'কয়েন কম');
    }

    user.coins -= RP[index];
    user.rings.push(index);
    user.ring = index;

    await putU(user);
    await sendMe();

    socket.emit('err', '✅ রিং কেনা হয়েছে!');
  });

  on('setring', async rawIndex => {
    const user = await requireUser();
    const index = Number(rawIndex);

    if (
      !user ||
      !(user.rings || [0]).includes(index)
    ) return;

    user.ring = index;

    await putU(user);
    await sendMe();
  });

  /* =================================
     PARTY FUNCTIONS
  ================================= */

  function leaveP() {
    const code = socket.data.pcode;
    const party = parties[code];

    if (!party) return;

    party.m = party.m.filter(
      p => p.sid !== socket.id
    );

    socket.leave('p' + code);
    delete socket.data.pcode;

    socket.emit('party', null);

    if (!party.m.length) {
      delete parties[code];
    } else {
      if (!party.m.some(p => p.uid === party.host)) {
        party.host = party.m[0].uid;
      }

      pubP(party);
    }
  }

  function joinP(code, user) {
    const party = parties[code];

    if (!party) {
      return socket.emit('err', 'পার্টি পাওয়া যায়নি');
    }

    if (party.m.some(p => p.uid === user._id)) {
      return;
    }

    if (party.m.length >= 4) {
      return socket.emit('err', 'পার্টি ভর্তি');
    }

    leaveP();

    party.m.push({
      sid: socket.id,
      uid: user._id,
      ...pi(user)
    });

    socket.join('p' + code);
    socket.data.pcode = code;

    pubP(party);
  }

  function makeParty(user) {
    const code = makeCode(4);

    parties[code] = {
      code,
      host: user._id,
      fee: 0,
      m: []
    };

    joinP(code, user);
  }

  on('joinparty', async rawCode => {
    const user = await requireUser();
    if (!user) return;

    joinP(
      String(rawCode || '').toUpperCase(),
      user
    );
  });

  on('acceptinv', async rawCode => {
    const user = await requireUser();
    if (!user) return;

    joinP(
      String(rawCode || '').toUpperCase(),
      user
    );
  });

  on('pfee', fee => {
    const party = parties[socket.data.pcode];

    if (
      !party ||
      party.host !== socket.data.uid ||
      !FEES.includes(Number(fee))
    ) return;

    party.fee = Number(fee);
    pubP(party);
  });

  /* =================================
     FRIENDS
  ================================= */

  on('addf', async rawId => {
    const user = await requireUser();
    if (!user) return;

    const target = await getU(
      String(rawId || '').trim()
    );

    if (!target) {
      return socket.emit('err', 'আইডি পাওয়া যায়নি');
    }

    if (target._id === user._id) {
      return socket.emit('err', 'নিজেকে অ্যাড করা যাবে না');
    }

    user.friends = user.friends || [];
    target.reqs = target.reqs || [];

    if (user.friends.includes(target._id)) {
      return socket.emit('err', 'আগেই বন্ধু');
    }

    if (target.reqs.includes(user._id)) {
      return socket.emit('err', 'রিকোয়েস্ট আগেই পাঠানো হয়েছে');
    }

    target.reqs.push(user._id);

    await putU(target);
    await sendF(target._id);

    socket.emit('err', 'রিকোয়েস্ট পাঠানো হয়েছে');
  });

  on('accept', async rawId => {
    const user = await requireUser();
    if (!user) return;

    const target = await getU(String(rawId));

    if (
      !target ||
      !(user.reqs || []).includes(target._id)
    ) return;

    user.reqs = user.reqs.filter(
      id => id !== target._id
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

  on('decl', async rawId => {
    const user = await requireUser();
    if (!user) return;

    user.reqs = (user.reqs || []).filter(
      id => id !== String(rawId)
    );

    await putU(user);
    await sendF(user._id);
  });

  on('invite', async rawId => {
    const user = await requireUser();
    if (!user) return;

    const friendId = String(rawId);
    const sid = online.get(friendId);

    if (
      !sid ||
      !(user.friends || []).includes(friendId)
    ) return;

    const code = socket.data.pcode;

    if (!code || !parties[code]) {
      return socket.emit('err', 'আগে পার্টি তৈরি করুন');
    }

    io.to(sid).emit('invite', {
      from: user.name,
      code
    });
  });

  /* =================================
     LUDO START
  ================================= */

  on('pstart', async () => {
    const party = parties[socket.data.pcode];

    if (
      !party ||
      party.host !== socket.data.uid ||
      socket.data.code
    ) return;

    if (party.m.length < 2) {
      return socket.emit('err', 'কমপক্ষে ২ জন লাগবে');
    }

    const playerData = [];

    for (const member of party.m) {
      const user = await getU(member.uid);

      if (!user || user.coins < party.fee) {
        return socket.emit('err', member.name + ' এর কয়েন কম');
      }

      playerData.push({ user, member });
    }

    const code = makeCode(4);

    const room = rooms[code] = {
      code,
      fee: party.fee,
      pot: party.fee * playerData.length,
      players: [],
      turn: 0,
      dice: 0,
      phase: 'roll',
      msg: 'খেলা শুরু',
      winner: null
    };

    for (let i = 0; i < playerData.length; i++) {
      const { user, member } = playerData[i];

      user.coins -= party.fee;
      await putU(user);

      room.players.push({
        id: member.sid,
        uid: user._id,
        ...pi(user),
        coins: user.coins,
        seat: SEATS[i],
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

    room.msg = 'খেলা শুরু! ' +
      room.players[0].name +
      ' আগে ফেলুক';

    pub(room);
  });

  /* =================================
     LUDO DICE
  ================================= */

  on('roll', () => {
    const room = rooms[socket.data.code];

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

      const dice = 1 + Math.floor(Math.random() * 6);

      room.dice = dice;

      if (legal(player, dice).length) {
        room.phase = 'move';
        room.msg = player.name + ' ' + dice + ' ফেলল';
      } else {
        room.phase = 'wait';
        room.msg = player.name + ' এর চাল নেই';

        setTimeout(() => {
          if (
            rooms[room.code] &&
            room.phase === 'wait'
          ) {
            next(room);
            room.msg = room.players[room.turn].name + ' এর পালা';
            pub(room);
          }
        }, 1500);
      }

      pub(room);
    }, 800);
  });

  on('move', async rawIndex => {
    const room = rooms[socket.data.code];
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

    const position = player.pieces[index] < 0
      ? 0
      : player.pieces[index] + dice;

    player.pieces[index] = position;

    let captured = false;

    if (
      position <= 50 &&
      !SAFE.includes(ab(player.seat, position))
    ) {
      for (const other of room.players) {
        if (other === player) continue;

        other.pieces.forEach((q, k) => {
          if (
            q >= 0 &&
            q <= 50 &&
            ab(other.seat, q) === ab(player.seat, position)
          ) {
            other.pieces[k] = -1;
            captured = true;
          }
        });
      }
    }

    if (player.pieces.every(p => p === 56)) {
      room.phase = 'over';
      room.winner = player.name;
      room.msg = '🏆 ' + player.name + ' জিতেছে!';

      if (room.pot > 0) {
        const user = await getU(player.uid);

        if (user) {
          user.coins += room.pot;
          await putU(user);
          player.coins = user.coins;

          io.to(player.id).emit('me', pu(user));
        }
      }

      pub(room);
      return;
    }

    if (dice === 6 || captured) {
      room.phase = 'roll';
      room.dice = 0;
      room.msg = captured
        ? 'গুটি কাটা হয়েছে! আবার ফেলুন'
        : player.name + ' আবার ফেলবে';
    } else {
      next(room);
      room.msg = room.players[room.turn].name + ' এর পালা';
    }

    pub(room);
  });

  on('lgame', () => {
    const room = rooms[socket.data.code];
    if (!room) return;

    const player = room.players.find(
      p => p.id === socket.id
    );

    socket.leave(room.code);
    delete socket.data.code;

    if (!player) return;

    player.left = true;

    if (room.players.every(p => p.left)) {
      delete rooms[room.code];
      return;
    }

    if (
      room.phase !== 'over' &&
      room.players[room.turn].id === socket.id
    ) {
      next(room);
    }

    room.msg = 'একজন খেলোয়াড় বের হয়ে গেছে';
    pub(room);
  });

  /* =================================
     OTHER GAMES
  ================================= */

  function leaveGameRoom() {
    const code = socket.data.gameRoom;
    const room = gameRooms[code];

    if (!room) {
      delete socket.data.gameRoom;
      return;
    }

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

  on('game:create', async (game, options = {}) => {
    const user = await requireUser();
    if (!user) return;

    if (!GAME_RULES[game]) {
      return socket.emit('err', 'গেম পাওয়া যায়নি');
    }

    leaveGameRoom();

    const room = createGameRoom(game, user, options);

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

    socket.emit('game:created', publicGameRoom(room));
    emitGameRoom(room);
  });

  on('game:join', async rawCode => {
    const user = await requireUser();
    if (!user) return;

    const code = String(rawCode || '').trim().toUpperCase();
    const room = gameRooms[code];

    if (!room) {
      return socket.emit('err', 'গেম রুম পাওয়া যায়নি');
    }

    if (room.status !== 'lobby') {
      return socket.emit('err', 'গেম শুরু হয়ে গেছে');
    }

    if (room.players.length >= room.max) {
      return socket.emit('err', 'রুম ভর্তি');
    }

    if (room.players.some(p => p.uid === user._id)) {
      return socket.emit('err', 'তুমি আগে থেকেই রুমে আছ');
    }

    leaveGameRoom();

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

    socket.emit('game:joined', publicGameRoom(room));
    emitGameRoom(room);
  });

  on('game:leave', () => leaveGameRoom());

  on('game:ready', ready => {
    const room = gameRooms[socket.data.gameRoom];
    if (!room || room.status !== 'lobby') return;

    const player = room.players.find(
      p => p.sid === socket.id
    );

    if (!player) return;

    player.ready = !!ready;
    emitGameRoom(room);
  });

  on('game:start', () => {
    const room = gameRooms[socket.data.gameRoom];

    if (
      !room ||
      room.host !== socket.data.uid ||
      room.status !== 'lobby'
    ) return;

    const rules = GAME_RULES[room.game];

    if (room.players.length < rules.min) {
      return socket.emit('err', 'আরও খেলোয়াড় লাগবে');
    }

    room.status = 'playing';
    room.startedAt = Date.now();

    emitGameRoom(room);

    emitRoom('g' + room.code, 'game:started', {
      game: room.game,
      code: room.code,
      map: room.map,
      startedAt: room.startedAt
    });
  });

  on('game:action', payload => {
    const room = gameRooms[socket.data.gameRoom];

    if (
      !room ||
      room.status !== 'playing' ||
      !payload ||
      typeof payload !== 'object' ||
      JSON.stringify(payload).length > 1500
    ) return;

    const player = room.players.find(
      p => p.sid === socket.id
    );

    if (!player || player.alive === false) return;

    emitRoom('g' + room.code, 'game:action', {
      uid: player.uid,
      name: player.name,
      action: payload.action,
      data: payload.data
    });
  });

  on('game:finish', result => {
    const room = gameRooms[socket.data.gameRoom];

    if (!room || room.status !== 'playing') return;

    const player = room.players.find(
      p => p.sid === socket.id
    );

    if (!player) return;

    player.finished = true;

    if (room.game === 'racing') {
      player.score = Math.max(
        player.score || 0,
        Number(result && result.score) || 0
      );
    }

    if (room.players.every(p => p.finished)) {
      room.status = 'finished';

      emitRoom(
        'g' + room.code,
        'game:finished',
        publicGameRoom(room)
      );
    }

    emitGameRoom(room);
  });

  on('game:eliminate', uid => {
    const room = gameRooms[socket.data.gameRoom];

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

    emitRoom('g' + room.code, 'game:combat-request', {
      from: attacker.uid,
      to: victim.uid
    });
  });

  /* =================================
     VOICE CHAT SIGNALING
  ================================= */

  on('sig', (to, data) => {
    if (
      typeof to !== 'string' ||
      !data ||
      typeof data !== 'object'
    ) return;

    const target = io.sockets.sockets.get(to);
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
     DISCONNECT
  ================================= */

  socket.on('disconnect', async () => {
    try {
      const code = socket.data.code;
      const room = rooms[code];

      if (room) {
        const player = room.players.find(
          p => p.id === socket.id
        );

        if (player) {
          player.left = true;

          if (room.players.every(p => p.left)) {
            delete rooms[code];
          } else {
            if (
              room.phase !== 'over' &&
              room.players[room.turn]?.id === socket.id
            ) {
              next(room);
            }

            pub(room);
          }
        }
      }

      leaveP();
      leaveGameRoom();

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

    } catch (error) {
      console.error('Disconnect error:', error.message);
    }
  });

});

/* =================================
   SERVER START
================================= */

srv.listen(PORT, () => {
  console.log(`RS LUDO server listening on ${PORT}`);
});

/* =================================
   SHUTDOWN
================================= */

process.on('SIGTERM', async () => {
  console.log('Server shutting down');

  try {
    if (mongoClient) {
      await mongoClient.close();
    }
  } catch (error) {
    console.error('MongoDB close error:', error.message);
  }

  srv.close(() => process.exit(0));
});
