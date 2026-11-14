const express=require('express'),http=require('http'),{Server}=require('socket.io'),{MongoClient}=require('mongodb');
const app=express(),srv=http.createServer(app),io=new Server(srv);
app.use(express.static('public'));
process.on('unhandledRejection',e=>console.log('unhandled',e&&e.message));
process.on('uncaughtException',e=>console.log('uncaught',e&&e.message));
const rooms={},parties={},online=new Map(),SEATS=[0,2,1,3],SAFE=[0,8,13,21,26,34,39,47],FEES=[0,50,100,200,500],RP=[0,100,300,500,800,1200,2000,5000];
const LAD={2:38,7:14,8:31,15:26,21:42,28:84,36:44,51:67,71:91,78:98,87:94},SNK={16:6,46:25,49:11,62:19,64:60,74:53,89:68,92:88,95:75,99:80};

/* ---------- ডাটাবেস ---------- */
const ENVS=['MONGODB_URI','MONGO_URL','MONGO_URI','DATABASE_URL'],EN=ENVS.find(k=>process.env[k]),DBU=EN?process.env[EN].trim():'',DBN=process.env.LUDO_DB_NAME||'rsludo';
let col=null,dbErr=DBU?'সংযোগ চলছে...':'কোনো ডাটাবেস ভ্যারিয়েবল পাওয়া যায়নি (MONGODB_URI)';
const mem=new Map();
const hint=m=>/auth/i.test(m)?'ইউজারনেম বা পাসওয়ার্ড ভুল। পাসওয়ার্ডে বিশেষ চিহ্ন থাকলে Atlas-এ শুধু অক্ষর-সংখ্যার পাসওয়ার্ড দিন':
 /ENOTFOUND|querySrv|ECONNREFUSED/i.test(m)?'কানেকশন লিংকের ঠিকানা ভুল, Atlas থেকে আবার কপি করুন':
 /select|timed out|ReplicaSet|whitelist|network/i.test(m)?'Atlas-এর Network Access এ 0.0.0.0/0 (Allow from anywhere) যোগ করুন':'';
async function dbConnect(){
 if(!DBU)return;
 try{
  const c=new MongoClient(DBU,{serverSelectionTimeoutMS:10000});
  await c.connect();col=c.db(DBN).collection('users');dbErr='';
  console.log('DB ok');io.emit('db',{ok:true,env:true})}
 catch(e){
  col=null;const m=String(e&&e.message||e);
  dbErr=m.slice(0,160)+' | '+hint(m);console.log('DB error',m);
  setTimeout(dbConnect,30000)}}
dbConnect();
app.get('/status',(q,r)=>r.type('text').send(
 'ডাটাবেস: '+(col?'✅ সংযুক্ত':'❌ সংযুক্ত নয়')+
 '\nভ্যারিয়েবল: '+(EN||'পাওয়া যায়নি')+
 '\nসমস্যা: '+(dbErr||'নেই')+
 (/[<>]/.test(DBU)?'\n⚠️ লিংকে < > চিহ্ন রয়ে গেছে: <USERNAME> আর <PASSWORD> আসল নাম-পাসওয়ার্ড দিয়ে বদলান, < > মুছে দিন':'')+
 '\nমেমোরিতে অ্যাকাউন্ট: '+mem.size));
const getU=async id=>col?await col.findOne({_id:id}):mem.get(id);
const putU=async u=>{if(col)await col.replaceOne({_id:u._id},u,{upsert:true});else mem.set(u._id,u)};

/* ---------- সাহায্যকারী ---------- */
const pu=u=>({id:u._id,name:u.name,pic:u.pic||'',ring:u.ring||0,rings:u.rings||[0],coins:u.coins});
const pi=u=>({id:u._id,name:u.name,pic:u.pic||'',ring:u.ring||0});
const ab=(s,p)=>(s*13+p)%52;
const legal=(pl,d)=>pl.pieces.map((p,i)=>i).filter(i=>pl.pieces[i]<0?d==6:pl.pieces[i]+d<=56);
const done=pl=>pl.pieces.every(p=>p==56);
const mate=(r,pl)=>r.players.find(o=>o!==pl&&o.seat%2==pl.seat%2);
const ctl=(r,pl)=>r.team&&done(pl)?(mate(r,pl)||pl):pl;
function next(r){let n=0;do{r.turn=(r.turn+1)%r.players.length;n++}while(r.players[r.turn].left&&n<9);r.phase='roll';r.dice=0}
const pubP=p=>io.to('p'+p.code).emit('party',{code:p.code,host:p.host,fee:p.fee,m:p.m});
async function sendF(uid){
 const sid=online.get(uid);if(!sid)return;
 const u=await getU(uid);if(!u)return;
 const L=async a=>{const o=[];for(const i of a||[]){const x=await getU(i);if(x)o.push({...pi(x),online:online.has(i)})}return o};
 io.to(sid).emit('friends',{friends:await L(u.friends),reqs:await L(u.reqs)})}
const presence=async u=>{for(const f of u.friends||[])sendF(f)};

io.on('connection',s=>{
 s.emit('db',{ok:!!col,env:!!DBU});
 const R=()=>rooms[s.data.code],pub=r=>io.to(r.code).emit('state',r);
 const U=async()=>s.data.uid?await getU(s.data.uid):null;
 const sendMe=async()=>{const u=await U();if(u)s.emit('me',pu(u))};

 /* ---- পার্টি ---- */
 const leaveP=()=>{
  const c=s.data.pcode,p=parties[c];if(!p)return;
  p.m=p.m.filter(x=>x.sid!=s.id);s.leave('p'+c);delete s.data.pcode;s.emit('party',null);
  if(!p.m.length)delete parties[c];
  else{if(!p.m.some(x=>x.uid==p.host))p.host=p.m[0].uid;pubP(p)}};
 const joinP=(c,u)=>{
  const p=parties[c];
  if(!p)return s.emit('err','পার্টি পাওয়া যায়নি');
  if(s.data.pcode==c)return;
  if(p.m.length>=4)return s.emit('err','পার্টি ভর্তি');
  leaveP();
  p.m.push({sid:s.id,uid:u._id,...pi(u)});
  s.join('p'+c);s.data.pcode=c;pubP(p)};
 const mkP=u=>{
  const c=Math.random().toString(36).slice(2,6).toUpperCase();
  parties[c]={code:c,host:u._id,fee:0,m:[]};joinP(c,u)};
 const upd=u=>{
  const p=parties[s.data.pcode],m=p&&p.m.find(x=>x.uid==u._id);
  if(m){Object.assign(m,pi(u));pubP(p)}};
 const enter=async u=>{
  s.data.uid=u._id;online.set(u._id,s.id);
  s.emit('me',pu(u));if(!s.data.pcode)mkP(u);
  for(const r of Object.values(rooms)){
   const p=r.players.find(x=>x.uid==u._id&&!x.quit);
   if(p&&r.phase!='over'){p.id=s.id;p.left=false;clearTimeout(r.gc);s.join(r.code);s.data.code=r.code;pub(r);break}}
  sendF(u._id);presence(u)};

 /* ---- অ্যাকাউন্ট ---- */
 s.on('register',async name=>{
  name=String(name||'').replace(/[<>&"'`]/g,'').trim().slice(0,12);
  if(!name)return s.emit('err','নাম দিন');
  let id;
  try{do{id=String(100000+Math.floor(Math.random()*900000))}while(await getU(id))}
  catch(e){return s.emit('err','ডাটাবেস সমস্যা, একটু পরে চেষ্টা করুন')}
  const u={_id:id,name,pic:'',coins:1000,bonus:0,ring:0,rings:[0],friends:[],reqs:[]};
  try{await putU(u)}catch(e){return s.emit('err','ডাটাবেস সমস্যা, একটু পরে চেষ্টা করুন')}
  enter(u)});
 s.on('login',async id=>{
  let u;
  try{u=await getU(String(id||'').trim())}catch(e){return s.emit('badid',{keep:true})}
  if(!u)return s.emit('badid',{keep:!!DBU&&!col});
  enter(u)});
 s.on('pic',async d=>{
  const u=await U();
  if(!u||typeof d!='string'||d.length>30000||!/^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+\/=]+$/.test(d))return;
  u.pic=d;await putU(u);sendMe();upd(u)});
 s.on('bonus',async()=>{
  const u=await U();if(!u)return;
  if(u.coins>=100)return s.emit('err','কয়েন ১০০ এর কম হলে বোনাস পাবেন');
  if(Date.now()-u.bonus<864e5)return s.emit('err','বোনাস ২৪ ঘণ্টায় একবার');
  u.coins+=200;u.bonus=Date.now();await putU(u);sendMe();s.emit('err','🎁 ২০০ কয়েন পেয়েছেন!')});
 s.on('buy',async i=>{
  const u=await U();if(!u||!(i>=1&&i<RP.length))return;
  u.rings=u.rings||[0];
  if(u.rings.includes(i))return;
  if(u.coins<RP[i])return s.emit('err','কয়েন কম');
  u.coins-=RP[i];u.rings.push(i);u.ring=i;await putU(u);sendMe();upd(u);s.emit('err','✅ রিং কেনা হয়েছে!')});
 s.on('setring',async i=>{
  const u=await U();if(!u||!(u.rings||[0]).includes(i))return;
  u.ring=i;await putU(u);sendMe();upd(u)});

 /* ---- বন্ধু ---- */
 s.on('addf',async gid=>{
  const u=await U();if(!u)return;
  gid=String(gid||'').trim();
  const t=await getU(gid);
  if(!t)return s.emit('err','এই আইডি পাওয়া যায়নি');
  if(t._id==u._id)return s.emit('err','নিজেকে অ্যাড করা যায় না');
  t.reqs=t.reqs||[];u.friends=u.friends||[];
  if(u.friends.includes(t._id))return s.emit('err','আগেই বন্ধু');
  if(t.reqs.includes(u._id))return s.emit('err','রিকোয়েস্ট আগেই পাঠানো');
  t.reqs.push(u._id);await putU(t);sendF(t._id);
  s.emit('err','📨 রিকোয়েস্ট পাঠানো হয়েছে')});
 s.on('accept',async id=>{
  const u=await U(),t=await getU(String(id));
  if(!u||!t||!(u.reqs||[]).includes(t._id))return;
  u.reqs=u.reqs.filter(x=>x!=t._id);
  u.friends=u.friends||[];t.friends=t.friends||[];
  if(!u.friends.includes(t._id))u.friends.push(t._id);
  if(!t.friends.includes(u._id))t.friends.push(u._id);
  await putU(u);await putU(t);sendF(u._id);sendF(t._id)});
 s.on('decl',async id=>{
  const u=await U();if(!u)return;
  u.reqs=(u.reqs||[]).filter(x=>x!=String(id));await putU(u);sendF(u._id)});
 s.on('invite',async fid=>{
  const u=await U(),t=online.get(String(fid));
  if(!u||!t||!(u.friends||[]).includes(String(fid)))return;
  if(!s.data.pcode)mkP(u);
  io.to(t).emit('invite',{from:u.name,code:s.data.pcode});
  s.emit('err','📨 ইনভাইট পাঠানো হয়েছে')});
 s.on('acceptinv',async c=>{const u=await U();if(u)joinP(String(c).toUpperCase(),u)});
 s.on('joinparty',async c=>{const u=await U();if(u)joinP(String(c).toUpperCase(),u)});
 s.on('pfee',f=>{
  const p=parties[s.data.pcode];
  if(!p||!FEES.includes(f)||!p.m.length||p.host!=s.data.uid)return;
  p.fee=f;pubP(p)});

 /* ---- পার্টি থেকে গেম শুরু (লুডু / টিম লুডু / সাপ-লুডু) ---- */
 s.on('pstart',async g=>{
  const p=parties[s.data.pcode];
  if(!p||p.host!=s.data.uid||s.data.code)return;
  if(p.m.length<2)return s.emit('err','কমপক্ষে ২ জন লাগবে');
  if(g=='team'&&p.m.length!=4)return s.emit('err','টিম মোডে ঠিক ৪ জন লাগবে');
  const us=[];
  for(const m of p.m){
   const x=await getU(m.uid);
   if(!x||x.coins<p.fee)return s.emit('err',m.name+' এর কয়েন কম');
   us.push(x)}
  const c=Math.random().toString(36).slice(2,6).toUpperCase();
  const r=rooms[c]={code:c,game:g=='snake'?'snake':'ludo',team:g=='team',fee:p.fee,pot:p.fee*us.length,players:[],turn:0,dice:0,phase:'roll',msg:'',winner:null};
  for(let k=0;k<us.length;k++){
   const x=us[k],m=p.m[k];
   x.coins-=p.fee;await putU(x);
   r.players.push({...pi(x),id:m.sid,uid:x._id,coins:x.coins,seat:SEATS[k],pieces:[-1,-1,-1,-1],pos:1});
   const sk=io.sockets.sockets.get(m.sid);
   if(sk){sk.join(c);sk.data.code=c;sk.emit('me',pu(x))}}
  r.msg='খেলা শুরু! '+r.players[0].name+' আগে ফেলুক';pub(r)});

 /* ---- ভয়েস সিগন্যাল (লবি + খেলা) ---- */
 s.on('sig',(to,data)=>{
  const t=io.sockets.sockets.get(to);
  if(!t)return;
  const ok=(t.data.code&&t.data.code===s.data.code)||(t.data.pcode&&t.data.pcode===s.data.pcode);
  if(ok)t.emit('sig',s.id,data)});

 /* ---- খেলা ---- */
 const payout=async(r,list)=>{
  if(!r.pot)return;
  const each=Math.floor(r.pot/list.length);
  for(const pl of list){
   const u=await getU(pl.uid);if(!u)continue;
   u.coins+=each;await putU(u);pl.coins=u.coins;
   io.to(pl.id).emit('me',pu(u))}
  r.msg='🏆 '+r.winner+' জিতেছে! 🪙+'+each+(list.length>1?' (প্রত্যেকে)':'');pub(r)};
 const snk=(r,pl,d)=>{
  let m=pl.pos+d,t='';
  if(m>100){m=pl.pos;t=' (১০০ এর বেশি, চাল নেই)'}
  else if(LAD[m]){m=LAD[m];t=' 🪜 মই!'}
  else if(SNK[m]){m=SNK[m];t=' 🐍 সাপে কামড়!'}
  pl.pos=m;
  if(m==100){r.phase='over';r.winner=pl.name;r.msg='🏆 '+pl.name+' জিতেছে!';pub(r);payout(r,[pl]);return}
  r.msg=pl.name+' '+d+' ফেলল'+t;
  if(d==6){r.phase='roll';r.dice=0;r.msg+=' — আবার ফেলবে'}
  else{r.phase='wait';
   setTimeout(()=>{if(rooms[r.code]&&r.phase=='wait'){next(r);r.msg=r.players[r.turn].name+' এর পালা';pub(r)}},1500)}
  pub(r)};
 s.on('roll',()=>{
  const r=R();
  if(!r||r.phase!='roll'||r.players[r.turn].uid!=s.data.uid)return;
  const pl=r.players[r.turn];
  r.phase='rolling';r.msg=pl.name+' ডাইস ফেলছে...';pub(r);
  setTimeout(()=>{
   if(!rooms[r.code]||r.phase!='rolling')return;
   const d=1+Math.floor(Math.random()*6);r.dice=d;
   if(r.game=='snake')return snk(r,pl,d);
   const lg=pl.left?[]:legal(ctl(r,pl),d);
   if(lg.length){r.phase='move';r.msg=pl.name+' '+d+' ফেলল'+(d==6?' 🔥 ছক্কা!':'')}
   else{r.phase='wait';r.msg=pl.name+' '+d+' ফেলল, চাল নেই 😅';
    setTimeout(()=>{if(rooms[r.code]&&r.phase=='wait'){next(r);r.msg=r.players[r.turn].name+' এর পালা';pub(r)}},1500)}
   pub(r)},800)});
 s.on('move',i=>{
  const r=R();
  if(!r||r.game!='ludo'||r.phase!='move'||r.players[r.turn].uid!=s.data.uid)return;
  const pl=r.players[r.turn],d=r.dice,tp=ctl(r,pl);
  if(!legal(tp,d).includes(i))return;
  const np=tp.pieces[i]<0?0:tp.pieces[i]+d;tp.pieces[i]=np;let cap=null;
  if(np<=50&&!SAFE.includes(ab(tp.seat,np)))
   r.players.forEach(o=>{
    if(o===tp||(r.team&&o.seat%2==tp.seat%2))return;
    o.pieces.forEach((q,k)=>{if(q>=0&&q<=50&&ab(o.seat,q)==ab(tp.seat,np)){o.pieces[k]=-1;cap=o.name}})});
  const grp=r.team?r.players.filter(o=>o.seat%2==tp.seat%2):[tp];
  if(grp.every(o=>done(o))){
   r.phase='over';r.winner=grp.map(o=>o.name).join(' + ');
   r.msg='🏆 '+r.winner+' জিতেছে!';pub(r);payout(r,grp);return}
  if(d==6||cap){r.phase='roll';r.dice=0;
   r.msg=cap?'😂 '+cap+' এর গুটি কাটা পড়ল! '+pl.name+' আবার ফেলবে':pl.name+' আবার ফেলবে'}
  else{next(r);r.msg=r.players[r.turn].name+' এর পালা'}
  pub(r)});

 const leaveGame=vol=>{
  const r=R();if(!r)return;
  const k=r.players.findIndex(p=>p.id==s.id);
  s.leave(r.code);delete s.data.code;
  if(k<0)return;
  const p=r.players[k];p.left=true;if(vol)p.quit=true;
  if(r.players.every(x=>x.left)){
   if(r.players.every(x=>x.quit))delete rooms[r.code];
   else r.gc=setTimeout(()=>{if(rooms[r.code]&&r.players.every(x=>x.left))delete rooms[r.code]},120000);
   return}
  if(['roll','move'].includes(r.phase)&&r.turn==k)next(r);
  r.msg=p.name+(vol?' বের হয়েছে':' ডিসকানেক্ট হয়েছে');pub(r)};
 s.on('lgame',()=>leaveGame(1));
 s.on('disconnect',async()=>{
  leaveGame(0);leaveP();
  const id=s.data.uid;
  if(id&&online.get(id)==s.id){online.delete(id);const u=await getU(id);if(u)presence(u)}});
});

/* ================= মাল্টিপ্লেয়ার রেসিং ================= */
(()=>{
 const races={},PRIZE=[60,40,25,15];
 const pub=r=>io.to('r'+r.code).emit('rs',{code:r.code,host:r.host,map:r.map,state:r.state,
  ps:r.ps.map(({uid,name,pic,ring,car,ready,fin,on})=>({uid,name,pic,ring,car,ready,fin,on:!!on}))});
 const mk=(host,list)=>{
  const c=Math.random().toString(36).slice(2,6).toUpperCase();
  return races[c]={code:c,host,map:0,state:'lobby',n:0,
   ps:list.map(m=>({uid:m.uid,name:m.name,pic:m.pic||'',ring:m.ring||0,car:0,ready:false,fin:null,sid:null,on:false}))}};
 io.on('connection',s=>{
  const R=()=>races[s.data.rcode],P=r=>r&&r.ps.find(p=>p.uid==s.data.uid);
  const join=r=>{
   const p=r.ps.find(x=>x.uid==s.data.uid);
   if(!p)return s.emit('rgone');
   p.sid=s.id;p.on=true;s.join('r'+r.code);s.data.rcode=r.code;pub(r)};

  s.on('prace',()=>{
   const p=parties[s.data.pcode];if(!p)return;
   if(p.host!=s.data.uid)return s.emit('err','রেস শুরু করবেন পার্টির হোস্ট');
   const r=mk(p.host,p.m);
   p.m.forEach(m=>io.to(m.sid).emit('goto','/race.html?room='+r.code))});
  s.on('rjoin',c=>{const r=races[String(c).toUpperCase()];r?join(r):s.emit('rgone')});
  s.on('rsolo',async()=>{
   const u=await getU(s.data.uid);if(!u)return;
   join(mk(u._id,[{uid:u._id,name:u.name,pic:u.pic,ring:u.ring}]))});
  s.on('rcar',i=>{
   const r=R(),p=P(r);
   if(!r||!p||r.state!='lobby'||!(i>=0&&i<6))return;
   p.car=i;p.ready=false;pub(r)});
  s.on('rmap',i=>{
   const r=R();
   if(!r||r.state!='lobby'||r.host!=s.data.uid||!(i>=0&&i<5))return;
   r.map=i;r.ps.forEach(p=>p.ready=false);pub(r)});
  s.on('rready',()=>{
   const r=R(),p=P(r);
   if(!r||!p||r.state!='lobby')return;
   p.ready=!p.ready;
   const on=r.ps.filter(x=>x.on);
   if(on.length&&on.every(x=>x.ready)){
    r.state='race';r.n=0;
    io.to('r'+r.code).emit('rgo',{map:r.map,ps:on.map(x=>({uid:x.uid,name:x.name,car:x.car}))})}
   pub(r)});
  s.on('rp',d=>{
   const r=R();if(!r||r.state!='race'||!d)return;
   s.to('r'+r.code).emit('rp',{u:s.data.uid,z:+d.z||0,x:+d.x||0,v:+d.v||0})});
  s.on('rfin',async ms=>{
   const r=R(),p=P(r);
   if(!r||!p||r.state!='race'||p.fin!=null)return;
   ms=+ms;if(!(ms>8000))return;
   p.fin=ms;p.rank=++r.n;
   const u=await getU(p.uid);
   if(u&&Date.now()-(u.race||0)>6e4){
    const pr=r.ps.length>1?PRIZE[Math.min(p.rank-1,3)]:20;
    u.race=Date.now();u.coins+=pr;await putU(u);p.prize=pr;
    io.to(s.id).emit('me',pu(u))}
   if(r.ps.filter(x=>x.on).every(x=>x.fin!=null))r.state='done';
   io.to('r'+r.code).emit('rres',r.ps.filter(x=>x.fin!=null).sort((a,b)=>a.rank-b.rank).map(x=>({name:x.name,fin:x.fin,prize:x.prize||0})));
   pub(r)});
  s.on('rreset',()=>{
   const r=R();
   if(!r||r.host!=s.data.uid||r.state=='race')return;
   r.state='lobby';r.ps.forEach(p=>{p.ready=false;p.fin=null;p.rank=0;p.prize=0});pub(r)});
  s.on('disconnect',()=>{
   const r=R(),p=P(r);if(!r||!p)return;
   p.on=false;p.sid=null;
   const on=r.ps.filter(x=>x.on);
   if(!on.length){delete races[r.code];return}
   if(r.host==p.uid)r.host=on[0].uid;
   if(r.state=='race'&&on.every(x=>x.fin!=null))r.state='done';
   pub(r)});
 });
})();

srv.listen(process.env.PORT||3000);
