const express=require('express'),http=require('http'),{Server}=require('socket.io');
const app=express(),srv=http.createServer(app),io=new Server(srv);
app.use(express.static('public'));
const rooms={},SEATS=[0,2,1,3],SAFE=[0,8,13,21,26,34,39,47];
const ab=(s,p)=>(s*13+p)%52;
const legal=(pl,d)=>pl.pieces.map((p,i)=>i).filter(i=>pl.pieces[i]<0?d==6:pl.pieces[i]+d<=56);
function next(r){let n=0;do{r.turn=(r.turn+1)%r.players.length;n++}while(r.players[r.turn].left&&n<9);r.phase='roll';r.dice=0}
io.on('connection',s=>{
 const R=()=>rooms[s.data.code],pub=r=>io.to(r.code).emit('state',r);
 const join=(r,name)=>{
  if(r.phase!='lobby')return s.emit('err','খেলা শুরু হয়ে গেছে');
  if(r.players.length>=4)return s.emit('err','রুম ভর্তি');
  name=(name||'খেলোয়াড়').slice(0,12);
  r.players.push({id:s.id,name,seat:SEATS[r.players.length],pieces:[-1,-1,-1,-1]});
  s.join(r.code);s.data.code=r.code;r.msg=name+' জয়েন করেছে';pub(r)};
 s.on('create',name=>{const c=Math.random().toString(36).slice(2,6).toUpperCase();
  rooms[c]={code:c,players:[],turn:0,dice:0,phase:'lobby',msg:'',winner:null};join(rooms[c],name)});
 s.on('join',(c,name)=>{const r=rooms[(c||'').toUpperCase()];r?join(r,name):s.emit('err','রুম পাওয়া যায়নি')});
 s.on('sig',(to,data)=>{
  const t=io.sockets.sockets.get(to);
  if(t&&t.data.code&&t.data.code===s.data.code)t.emit('sig',s.id,data)});
 s.on('start',()=>{const r=R();
  if(!r||r.phase!='lobby'||r.players[0].id!=s.id||r.players.length<2)return;
  r.phase='roll';r.msg='খেলা শুরু! '+r.players[0].name+' আগে ফেলুক';pub(r)});
 s.on('roll',()=>{const r=R();
  if(!r||r.phase!='roll'||r.players[r.turn].id!=s.id)return;
  const pl=r.players[r.turn],d=1+Math.floor(Math.random()*6);r.dice=d;
  if(legal(pl,d).length){r.phase='move';r.msg=pl.name+' '+d+' ফেলল'+(d==6?' 🔥 ছক্কা!':'')}
  else{r.phase='wait';r.msg=pl.name+' '+d+' ফেলল, চাল নেই 😅';
   setTimeout(()=>{if(rooms[r.code]&&r.phase=='wait'){next(r);r.msg=r.players[r.turn].name+' এর পালা';pub(r)}},1500)}
  pub(r)});
 s.on('move',i=>{const r=R();
  if(!r||r.phase!='move'||r.players[r.turn].id!=s.id)return;
  const pl=r.players[r.turn],d=r.dice;
  if(!legal(pl,d).includes(i))return;
  const np=pl.pieces[i]<0?0:pl.pieces[i]+d;pl.pieces[i]=np;let cap=null;
  if(np<=50&&!SAFE.includes(ab(pl.seat,np)))
   r.players.forEach(o=>{if(o!==pl)o.pieces.forEach((q,k)=>{
    if(q>=0&&q<=50&&ab(o.seat,q)==ab(pl.seat,np)){o.pieces[k]=-1;cap=o.name}})});
  if(pl.pieces.every(p=>p==56)){r.phase='over';r.winner=pl.name;r.msg='🏆 '+pl.name+' জিতেছে!'}
  else if(d==6||cap){r.phase='roll';r.dice=0;
   r.msg=cap?'😂 '+cap+' এর গুটি কাটা পড়ল! '+pl.name+' আবার ফেলবে':pl.name+' আবার ফেলবে'}
  else{next(r);r.msg=r.players[r.turn].name+' এর পালা'}
  pub(r)});
 s.on('disconnect',()=>{const r=R();if(!r)return;
  const k=r.players.findIndex(p=>p.id==s.id);if(k<0)return;
  if(r.phase=='lobby')r.players.splice(k,1);else r.players[k].left=true;
  if(!r.players.length||r.players.every(p=>p.left)){delete rooms[r.code];return}
  if(r.phase!='lobby'&&r.phase!='over'&&r.turn==k)next(r);
  r.msg='একজন বের হয়ে গেছে';pub(r)});
});
srv.listen(process.env.PORT||3000);
