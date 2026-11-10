module.exports=(io,getU,putU,pu,parties)=>{
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
};
