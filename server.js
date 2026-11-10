const express=require('express'),http=require('http'),{Server}=require('socket.io'),{MongoClient}=require('mongodb');
const app=express(),srv=http.createServer(app),io=new Server(srv);
app.use(express.static('public'));
const rooms={},SEATS=[0,2,1,3],SAFE=[0,8,13,21,26,34,39,47],FEES=[0,50,100,200,500];
let col=null;const mem=new Map();
if(process.env.MONGO_URL)new MongoClient(process.env.MONGO_URL).connect().then(c=>{col=c.db('rsludo').collection('users');console.log('DB ok')}).catch(e=>console.log('DB error',e.message));
const getU=async id=>col?await col.findOne({_id:id}):mem.get(id);
const putU=async u=>{if(col)await col.replaceOne({_id:u._id},u,{upsert:true});else mem.set(u._id,u)};
const pu=u=>({id:u._id,name:u.name,pic:u.pic||'',coins:u.coins});
const ab=(s,p)=>(s*13+p)%52;
const legal=(pl,d)=>pl.pieces.map((p,i)=>i).filter(i=>pl.pieces[i]<0?d==6:pl.pieces[i]+d<=56);
function next(r){let n=0;do{r.turn=(r.turn+1)%r.players.length;n++}while(r.players[r.turn].left&&n<9);r.phase='roll';r.dice=0}
io.on('connection',s=>{
 const R=()=>rooms[s.data.code],pub=r=>io.to(r.code).emit('state',r);
 const U=async()=>s.data.uid?await getU(s.data.uid):null;
 const sendMe=async()=>{const u=await U();if(u)s.emit('me',pu(u))};

 s.on('register',async name=>{
  name=String(name||'').replace(/[<>&"'`]/g,'').trim().slice(0,12);
  if(!name)return s.emit('err','নাম দিন');
  let id;do{id=String(100000+Math.floor(Math.random()*900000))}while(await getU(id));
  const u={_id:id,name,pic:'',coins:1000,bonus:0};
  await putU(u);s.data.uid=id;s.emit('me',pu(u))});
 s.on('login',async id=>{
  const u=await getU(String(id||'').trim());
  if(!u)return s.emit('badid');
  s.data.uid=u._id;s.emit('me',pu(u))});
 s.on('pic',async d=>{
  const u=await U();
  if(!u||typeof d!='string'||d.length>30000||!/^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+\/=]+$/.test(d))return;
  u.pic=d;await putU(u);sendMe()});
 s.on('bonus',async()=>{
  const u=await U();if(!u)return;
  if(u.coins>=100)return s.emit('err','কয়েন ১০০ এর কম হলে বোনাস পাবেন');
  if(Date.now()-u.bonus<864e5)return s.emit('err','বোনাস ২৪ ঘণ্টায় একবার');
  u.coins+=200;u.bonus=Date.now();await putU(u);sendMe();s.emit('err','🎁 ২০০ কয়েন পেয়েছেন!')});

 const join=async r=>{
  const u=await U();if(!u)return s.emit('err','আগে লগইন করুন');
  if(r.phase!='lobby')return s.emit('err','খেলা শুরু হয়ে গেছে');
  if(r.players.length>=4)return s.emit('err','রুম ভর্তি');
  if(r.players.some(p=>p.uid==u._id))return s.emit('err','আপনি আগেই ঢুকেছেন');
  if(u.coins<r.fee)return s.emit('err','কয়েন কম, এন্ট্রি '+r.fee);
  r.players.push({id:s.id,uid:u._id,name:u.name,pic:u.pic||'',coins:u.coins,seat:SEATS[r.players.length],pieces:[-1,-1,-1,-1]});
  s.join(r.code);s.data.code=r.code;r.msg=u.name+' জয়েন করেছে';pub(r)};
 s.on('create',async fee=>{
  const c=Math.random().toString(36).slice(2,6).toUpperCase();
  const r=rooms[c]={code:c,fee:FEES.includes(fee)?fee:0,pot:0,players:[],turn:0,dice:0,phase:'lobby',msg:'',winner:null};
  await join(r);if(!r.players.length)delete rooms[c]});
 s.on('join',c=>{const r=rooms[String(c||'').toUpperCase()];r?join(r):s.emit('err','রুম পাওয়া যায়নি')});
 s.on('sig',(to,data)=>{
  const t=io.sockets.sockets.get(to);
  if(t&&t.data.code&&t.data.code===s.data.code)t.emit('sig',s.id,data)});

 s.on('start',async()=>{
  const r=R();
  if(!r||r.phase!='lobby'||r.players[0].id!=s.id||r.players.length<2)return;
  r.phase='starting';const us=[];
  for(const p of r.players){
   const u=await getU(p.uid);
   if(!u||u.coins<r.fee){r.phase='lobby';return s.emit('err',p.name+' এর কয়েন কম')}
   us.push(u)}
  for(let k=0;k<us.length;k++){
   us[k].coins-=r.fee;await putU(us[k]);r.players[k].coins=us[k].coins;
   io.to(r.players[k].id).emit('me',pu(us[k]))}
  r.pot=r.fee*r.players.length;r.phase='roll';
  r.msg='খেলা শুরু! '+r.players[0].name+' আগে ফেলুক';pub(r)});

 s.on('roll',()=>{
  const r=R();
  if(!r||r.phase!='roll'||r.players[r.turn].id!=s.id)return;
  const pl=r.players[r.turn];
  r.phase='rolling';r.msg=pl.name+' ডাইস ফেলছে...';pub(r);
  setTimeout(()=>{
   if(!rooms[r.code]||r.phase!='rolling')return;
   const d=1+Math.floor(Math.random()*6);r.dice=d;
   if(legal(pl,d).length){r.phase='move';r.msg=pl.name+' '+d+' ফেলল'+(d==6?' 🔥 ছক্কা!':'')}
   else{r.phase='wait';r.msg=pl.name+' '+d+' ফেলল, চাল নেই 😅';
    setTimeout(()=>{if(rooms[r.code]&&r.phase=='wait'){next(r);r.msg=r.players[r.turn].name+' এর পালা';pub(r)}},1500)}
   pub(r)},800)});

 const payout=async(r,pl)=>{
  if(!r.pot)return;
  const u=await getU(pl.uid);if(!u)return;
  u.coins+=r.pot;await putU(u);pl.coins=u.coins;
  r.msg='🏆 '+pl.name+' জিতেছে! 🪙+'+r.pot;
  io.to(pl.id).emit('me',pu(u));pub(r)};

 s.on('move',i=>{
  const r=R();
  if(!r||r.phase!='move'||r.players[r.turn].id!=s.id)return;
  const pl=r.players[r.turn],d=r.dice;
  if(!legal(pl,d).includes(i))return;
  const np=pl.pieces[i]<0?0:pl.pieces[i]+d;pl.pieces[i]=np;let cap=null;
  if(np<=50&&!SAFE.includes(ab(pl.seat,np)))
   r.players.forEach(o=>{if(o!==pl)o.pieces.forEach((q,k)=>{
    if(q>=0&&q<=50&&ab(o.seat,q)==ab(pl.seat,np)){o.pieces[k]=-1;cap=o.name}})});
  if(pl.pieces.every(p=>p==56)){r.phase='over';r.winner=pl.name;r.msg='🏆 '+pl.name+' জিতেছে!';pub(r);payout(r,pl);return}
  else if(d==6||cap){r.phase='roll';r.dice=0;
   r.msg=cap?'😂 '+cap+' এর গুটি কাটা পড়ল! '+pl.name+' আবার ফেলবে':pl.name+' আবার ফেলবে'}
  else{next(r);r.msg=r.players[r.turn].name+' এর পালা'}
  pub(r)});

 s.on('disconnect',()=>{
  const r=R();if(!r)return;
  const k=r.players.findIndex(p=>p.id==s.id);if(k<0)return;
  if(r.phase=='lobby')r.players.splice(k,1);else r.players[k].left=true;
  if(!r.players.length||r.players.every(p=>p.left)){delete rooms[r.code];return}
  if(r.phase!='lobby'&&r.phase!='over'&&r.turn==k)next(r);
  r.msg='একজন বের হয়ে গেছে';pub(r)});
});
srv.listen(process.env.PORT||3000);
