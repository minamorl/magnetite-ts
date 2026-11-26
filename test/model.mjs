import { initial, step, descriptions, successor } from '../src/model.ts';
import assert from 'node:assert/strict';

const mutant = process.env.MUTANT || '';
function checked(s,e) {
  const before=s.record?.pending.length ?? 0;
  let result=step(s,e);
  if(mutant==='notice' && result.effects.some(x=>x.type==='end')) result.effects=result.effects.filter(x=>x.type!=='end');
  if(mutant==='retention' && e.type==='send' && result.effects.some(x=>x.type==='result'&&x.result==='limit') && result.state.record) result.state={...result.state,record:{...result.state.record,pending:[]}};
  if(mutant==='terminal' && s.phase==='closed' && e.type==='send') result.effects=[{type:'result',result:'accepted',seq:1}];
  if(mutant==='ack' && result.effects.some(x=>x.type==='frame'&&x.kind==='ack')) result.effects=result.effects.map(x=>x.type==='frame'&&x.kind==='ack'?{...x,seq:x.seq+1}:x);
  if(mutant==='liveness' && e.type==='tick' && s.phase==='disconnected') return {state:{...s,now:e.now},effects:[]};
  if(mutant==='no-timeout' && e.type==='tick' && s.phase==='open' && result.state.phase==='disconnected') result={state:{...s,now:e.now},effects:[]};
  if(mutant==='no-pong') result.effects=result.effects.filter(x=>x.type!=='frame'||x.kind!=='pong');
  if(e.type==='tick' && s.phase==='open') assert(result.state.phase!=='open' || e.now-s.lastReceived<s.lossAfter,'チ: 無通信期限で切断');
  if(e.type==='tick' && s.phase==='open' && s.pongDue!==null && e.now>=s.pongDue && e.now-s.lastReceived<s.lossAfter) assert(result.effects.some(x=>x.type==='frame'&&x.kind==='pong'&&x.at===s.pongDue),'チ: pong を期限に返す');
  const ends=result.effects.filter(x=>x.type==='end');
  if(s.phase!=='closed' && result.state.phase==='closed') {
    assert.equal(ends.length,1,'ロ: 閉への遷移に終了通知が一度必要');
    if(ends[0].reason!=='replaced') assert.equal(ends[0].count,before,'ロ: 未 ack 件数');
  } else assert.equal(ends.length,0,'ロ: 終了通知の重複');
  if(e.type==='send' && result.effects.some(x=>x.type==='result'&&x.result==='limit')) assert.equal(result.state.record?.pending.length,before,'ハ: 枠超過で受理済み送信を捨てない');
  if(s.phase==='closed') { assert.equal(result.state.phase,'closed','ニ: 閉は終端'); assert(!result.effects.some(x=>x.type==='retry'||x.type==='frame'||x.type==='result'&&x.result==='accepted'),'ニ: 閉では受理せず自動動作しない'); }
  for(const x of result.effects) if(x.type==='frame'&&x.kind==='ack') assert(x.seq <= (s.record?.received ?? 0),'ホ: 未配送の連番を ack しない');
  return result;
}
function ready(role) { const s=initial(role); return checked(s,{type:'ready',socket:1}).state; }
function unit() {
 const edgeFailures=[];
 for(const [name,check] of [
  ['send safe integer limit',()=>{ for(const phase of ['open','disconnected']) { let s=ready('client'); if(phase==='disconnected') s=checked(s,{type:'loss',socket:1}).state; s={...s,record:{...s.record,sent:Number.MAX_SAFE_INTEGER}}; const result=checked(s,{type:'send',payload:'x'}); assert.strictEqual(result.state,s); assert.deepEqual(result.effects,[{type:'result',result:'limit'}]); } }],
  ['data invalid sequence',()=>{ for(const seq of [0,-1,1.5,NaN,Infinity,Number.MAX_SAFE_INTEGER+1]) { let s=ready('server'); s=checked(s,{type:'send',payload:'pending'}).state; const result=checked(s,{type:'data',socket:1,seq,payload:'x'}); assert.equal(result.state.phase,'closed',`seq=${seq}`); assert.deepEqual(result.effects,[{type:'end',reason:'protocol_error',count:1}]); assert.deepEqual(checked(s,{type:'data',socket:2,seq,payload:'x'}),{state:s,effects:[]}); } }],
 ]) { try { check(); } catch(e) { edgeFailures.push(`${name}: ${e.message}`); } }
 assert.deepEqual(edgeFailures,[],'sequence boundary regressions');
 let s=ready('client'); s={...s,maxCount:1}; s=checked(s,{type:'send',payload:'a'}).state; checked(s,{type:'send',payload:'b'});
 const closed=checked(s,{type:'close'}).state; checked(closed,{type:'send',payload:'x'}); checked(closed,{type:'close'});
 let recv=ready('server'); recv=checked(recv,{type:'data',socket:1,seq:1,payload:'x'}).state; checked(recv,{type:'tick',now:1000});
 let disconnected=ready('client'); disconnected=checked(disconnected,{type:'loss',socket:1}).state; disconnected=checked(disconnected,{type:'tick',now:600000}).state; assert.equal(disconnected.phase,'closed','ヘ: 時計を進めれば満了する');
 let waiting=ready('client'); waiting=checked(waiting,{type:'loss',socket:1}).state; waiting=checked(waiting,{type:'tick',now:599999}).state; waiting=checked(waiting,{type:'attempt',socket:2}).state; waiting=checked(waiting,{type:'tick',now:600000}).state; assert.equal(waiting.phase,'disconnected'); assert.equal(checked(waiting,{type:'send',payload:'late'}).effects[0].result,'not_open'); waiting=checked(waiting,{type:'tick',now:609999}).state; assert.equal(waiting.phase,'closed');
 let late=ready('client'); late=checked(late,{type:'loss',socket:1}).state; late=checked(late,{type:'tick',now:599999}).state; late=checked(late,{type:'attempt',socket:2}).state; late=checked(late,{type:'tick',now:600000}).state; late=checked(late,{type:'resume',socket:2,seq:0}).state; assert.equal(late.phase,'open','期限後でも試行期限前の成功応答は開');
 let c=initial('server'); assert.equal(checked(c,{type:'send',payload:'x'}).effects[0].result,'not_open'); c=checked(c,{type:'ack',seq:0,socket:1}).state; assert.equal(c.phase,'closed');
 let a=ready('client'); a=checked(a,{type:'loss',socket:1}).state; a=checked(a,{type:'attempt',socket:2}).state; a=checked(a,{type:'tick',now:10000}).state; assert.equal(a.attemptUntil,null); assert.equal(checked(a,{type:'resume',socket:2,seq:0}).state.phase,'disconnected');
 let b=initial('server'); b=checked(b,{type:'loss',socket:1}).state; assert.equal(b.phase,'closed'); assert.equal(b.record,null); assert.equal(checked(b,{type:'resume',socket:2,seq:0}).effects[0].reason,'unknown_identifier');
 let d=ready('server'); d={...d,expiryDuration:20}; d=checked(d,{type:'loss',socket:1}).state; d=checked(d,{type:'tick',now:20}).state; assert.equal(d.phase,'closed'); assert.equal(checked(d,{type:'resume',socket:2,seq:0}).effects[0].reason,'unknown_identifier');
 let z=ready('client'); z=checked(z,{type:'send',payload:'x'}).state; z=checked(z,{type:'loss',socket:1}).state; z=checked(z,{type:'ack',socket:1,seq:1}).state; assert.equal(z.record.pending.length,1); z=checked(z,{type:'attempt',socket:2}).state; z=checked(z,{type:'ack',socket:2,seq:1}).state; assert.equal(z.record.pending.length,1); z=checked(z,{type:'loss',socket:2}).state; assert.equal(z.attemptUntil,null);
 let p=ready('server'); p=checked(p,{type:'send',payload:'x'}).state; const replacement=checked(p,{type:'replace'}); assert.equal(replacement.effects.find(x=>x.type==='transfer').record.pending.length,1); assert.equal(replacement.effects.find(x=>x.type==='end').reason,'replaced'); assert.equal(replacement.effects.find(x=>x.type==='end').count,undefined);
 let old=ready('server'); const invalid=checked(old,{type:'resume',socket:2,seq:1}); assert.equal(invalid.state.phase,'open'); assert.equal(invalid.effects[0].reason,'invalid_sequence'); const valid=checked(old,{type:'resume',socket:2,seq:0}); assert.equal(valid.state.phase,'closed'); assert.equal(valid.effects.find(e=>e.type==='end').reason,'replaced'); assert(valid.effects.some(e=>e.type==='transfer'));
 for(const role of ['client','server']) for(const event of [{type:'resume',seq:1},{type:'reject'}]) {
  const current=checked(ready(role),{type:'tick',now:15000}).state;
  const stale=checked(current,{...event,socket:2});
  if(event.type==='reject' || role==='client') assert.strictEqual(stale.state,current);
  const received=checked(current,{...event,socket:1});
  assert.equal(received.state.lastReceived,15000,`${role} ${event.type} resets lastReceived`);
  assert.equal(received.state.pingSent,false,`${role} ${event.type} resets pingSent`);
  if(event.type==='resume' && role==='server') assert.deepEqual(received.effects,[{type:'resume_reject',socket:1,reason:'invalid_sequence'}]);
  else assert.deepEqual(received.effects,[]);
  assert.equal(checked(received.state,{type:'tick',now:30000}).state.phase,'open');
 }
 let q=ready('client'); q=checked(q,{type:'ack',socket:1,seq:1}).state; assert.equal(q.phase,'closed');
 let x=ready('server'); x=checked(x,{type:'data',socket:1,seq:1,payload:'x'}).state; assert.equal(checked(x,{type:'tick',now:5000}).effects[0].at,1000);
 let alive=ready('client'), peer=ready('server'); for(let i=1;i<=100;i++) { const now=i*16000-1000; let result=checked(alive,{type:'tick',now}); assert.equal(result.state.phase,'open'); assert(result.effects.some(x=>x.kind==='ping')); alive=result.state; peer=checked(peer,{type:'tick',now}).state; peer=checked(peer,{type:'ping',socket:1}).state; result=checked(peer,{type:'tick',now:now+1000}); assert(result.effects.some(x=>x.kind==='pong')); peer=result.state; alive=checked(alive,{type:'tick',now:now+1000}).state; alive=checked(alive,{type:'pong',socket:1}).state; assert.equal(alive.phase,'open'); assert.equal(peer.phase,'open'); }
 let heartbeat=ready('client'); let first=checked(heartbeat,{type:'tick',now:15000}); assert.equal(first.effects[0].kind,'ping'); assert.equal(checked(first.state,{type:'tick',now:16000}).effects.length,0); let lost=checked(first.state,{type:'tick',now:30000}); assert.equal(lost.state.phase,'disconnected'); assert.equal(lost.effects[0].type,'socket_close');
 let ping=ready('server'); assert.equal(checked(ping,{type:'ping',socket:2}).state.pongDue,null); ping=checked(ping,{type:'ping',socket:1}).state; assert.equal(checked(ping,{type:'tick',now:1000}).effects[0].kind,'pong');
 assert.equal(checked(initial('client'),{type:'tick',now:30000}).effects[0].reason,'connect_failed');
}
// Frames are delivered only when the test drains the corresponding direction.
function fixedNetwork(blocked, lostHandshake) {
 let states=[ready('client'),ready('server')], queues=[[],[]], delivered=[[],[]], sent=[['c1','c2'],['s1','s2']];
 function apply(side,event) {
  const before=states[side]; let result=checked(before,event);
  if(mutant==='fixed-retry-timeout' && event.type==='tick' && before.attemptUntil!==null && event.now>=before.attemptUntil) result={state:{...before,now:event.now},effects:[]};
  states[side]=result.state;
  if(mutant==='fixed-omit-resend' && event.type==='resume' && result.state.phase==='open') result.effects=result.effects.filter(x=>x.type!=='frame'||x.kind!=='data');
  for(const effect of result.effects) {
   if(effect.type==='frame') queues[side].push(effect);
   if(effect.type==='deliver') delivered[side].push(effect.payload);
   assert(effect.type!=='end' || effect.reason==='replaced','unexpected end');
  }
  return result;
 }
 function drain(allowed) {
  while(queues.some(q=>q.length)) for(let side=0;side<2;side++) {
   const frame=queues[side].shift(); if(!frame || !allowed(side,frame)) continue;
   apply(1-side,{type:frame.kind,socket:frame.socket,seq:frame.seq,payload:frame.payload});
  }
 }
 for(let side=0;side<2;side++) for(const payload of sent[side]) assert.equal(apply(side,{type:'send',payload}).effects[0].result,'accepted');
 assert(states.every(s=>s.phase==='open' && s.record.pending.length===2));
 drain(()=>false);
 const lostAt=[null,null];
 for(let now=1000;now<=61000 && lostAt.some(x=>x===null);now+=1000) {
  for(let side=0;side<2;side++) if(states[side].phase==='open') {
   const last=states[side].lastReceived, result=apply(side,{type:'tick',now});
   if(result.state.phase==='disconnected') {
    lostAt[side]=now;
    assert(now-last>=30000 && now-last<31000,`side ${side}: loss at ${now}, last received ${last}`);
    assert(result.effects.some(x=>x.type==='socket_close' && x.socket===1));
   }
  }
  drain(side=>!blocked.includes(side));
 }
 assert(states.every(s=>s.phase==='disconnected'),'both ends detect loss independently');
 assert(lostAt.every(x=>x!==null));
 let socket=1;
 function attempt(drop) {
  socket++; const c=apply(0,{type:'attempt',socket});
  assert.equal(c.state.attemptUntil,c.state.now+10000);
  if(drop==='request') return;
  const before=states[1], seq=before.record.received;
  const response=apply(1,{type:'resume',socket,seq:states[0].record.received});
  const transfer=response.effects.find(x=>x.type==='transfer');
  if(transfer) {
   const next=successor(before,transfer.record,socket); states[1]=next.state;
   for(const effect of next.effects) if(effect.type==='frame') queues[1].push(effect);
  }
  assert.equal(states[1].phase,'open');
  if(drop==='response') { queues=[[],[]]; return; }
  apply(0,{type:'resume',socket,seq});
 }
 if(lostHandshake) {
  attempt(lostHandshake);
  const until=states[0].attemptUntil;
  let result=apply(0,{type:'tick',now:until-1}); assert.equal(result.state.attemptUntil,until);
  result=apply(0,{type:'tick',now:until});
  assert.equal(result.state.phase,'disconnected'); assert.equal(result.state.attemptUntil,null);
  assert(result.effects.some(x=>x.type==='retry'));
 }
 attempt(null);
 assert(states.every(s=>s.phase==='open' && s.socket===socket));
 drain(()=>true);
 for(let side=0;side<2;side++) if(states[side].ackDue!==null) apply(side,{type:'tick',now:states[side].ackDue});
 drain(()=>true);
 for(let side=0;side<2;side++) {
  assert.deepEqual(delivered[1-side],sent[side],'resume delivers each accepted message once in order');
  assert.equal(states[side].record.pending.length,0,'resume clears pending acknowledgements');
 }
}
function fixedHeartbeat() {
 let states=[ready('client'),ready('server')];
 function apply(side,event) {
  const old=states[side]; let result=checked(old,event);
  if(mutant==='fixed-pong-no-receipt' && event.type==='pong') result.state={...result.state,lastReceived:old.lastReceived};
  states[side]=result.state;
  assert.equal(result.state.phase,'open','heartbeat keeps both ends open');
  assert(!result.effects.some(x=>x.type==='socket_close'||x.type==='end'),'heartbeat causes no loss or end');
  return result.effects;
 }
 for(let time=15000;time+1000<=3600000;time+=16000) {
  for(let side=0;side<2;side++) {
   const effects=apply(side,{type:'tick',now:time});
   assert(effects.some(x=>x.type==='frame' && x.kind==='ping' && x.at===time));
  }
  for(let side=0;side<2;side++) apply(side,{type:'ping',socket:1});
  for(let side=0;side<2;side++) {
   const effects=apply(side,{type:'tick',now:time+1000});
   assert(effects.some(x=>x.type==='frame' && x.kind==='pong' && x.at===time+1000));
  }
  for(let side=0;side<2;side++) apply(side,{type:'pong',socket:1});
 }
 for(let side=0;side<2;side++) apply(side,{type:'tick',now:3600000});
}
function fixedTimeoutAndClose() {
 const cases=[
  ['attempt timeout retries after socket close',()=>{
   let s=ready('client'); s=checked(s,{type:'loss',socket:1}).state; s=checked(s,{type:'attempt',socket:2}).state;
   const result=checked(s,{type:'tick',now:10000});
   assert.deepEqual(result.effects,[{type:'socket_close',socket:2},{type:'retry'}]);
   assert.equal(result.state.socket,null); assert.equal(result.state.attemptUntil,null);
  }],
  ['attempt timeout expires after socket close',()=>{
   let s={...ready('client'),expiryDuration:5000}; s=checked(s,{type:'loss',socket:1}).state; s=checked(s,{type:'attempt',socket:2}).state;
   const result=checked(s,{type:'tick',now:10000});
   assert.deepEqual(result.effects,[{type:'socket_close',socket:2},{type:'end',reason:'expired',count:0}]);
   assert.equal(result.state.phase,'closed');
  }],
  ['closing clears scheduled pong',()=>{
   let s=ready('server'); s=checked(s,{type:'ping',socket:1}).state; assert.equal(s.pongDue,1000);
   assert.equal(checked(s,{type:'close'}).state.pongDue,null);
  }],
 ];
 const failures=[];
 for(const [name,check] of cases) { try { check(); console.log(`PASS fixed: ${name}`); } catch(e) { console.error(`FAIL fixed: ${name}\n${e.message}`); failures.push(name); } }
 assert.deepEqual(failures,[],'fixed timeout and close regressions');
}
function fixedTests() {
 fixedNetwork([0,1],null);
 for(const blocked of [[0],[1]]) for(const drop of ['request','response']) fixedNetwork(blocked,drop);
 fixedHeartbeat();
 console.log('PASS fixed: bidirectional silence, one-way silence and lost resume request/response, one-hour heartbeat');
}
function rng(seed) { let x=seed>>>0; return ()=>{x^=x<<13;x^=x>>>17;x^=x<<5;return x>>>0;}; }
const coverage = new Map();
function hit(key) { coverage.set(key,(coverage.get(key)||0)+1); }
function actions(seed) {
 const random=rng(seed), list=[], length=24+random()%57;
 for(let i=0;i<length;i++) { const n=random()%100; list.push({op:n<65?1:n<75?2+random()%2:[0,4,5,6,7,8,9,10,11,12,13,14][random()%12],side:random()%2,variant:random()%6}); }
 return list;
}
function scenario(list, countCoverage=true) {
 let states=[initial('client'),initial('server')], queues=[[],[{kind:'identifier',socket:1}]], sent=[[],[]], delivered=[[],[]], notices=[new Set(),new Set()], socket=1, response=null, request=null, uninterrupted=true, dead=new Set(), nextId=0;
 states=states.map((s,i)=>({...s,maxCount:1+(list[0]?.variant||0)%3,maxBytes:80+20*i}));
 const mark=k=>{if(countCoverage) hit(k);};
 const seen=new Set(); let resumed=false, replaced=false, finalizing=false; const silent=[new Set(),new Set()]; let silentlyLost=false;
 function apply(side,event) {
  if(mutant==='silent' && event.type==='close' && states[side].record?.pending.length) states[side]={...states[side],record:{...states[side].record,pending:states[side].record.pending.slice(0,-1)}};
  const pending=states[side].record?.pending.map(m=>m.payload) ?? [];
  const before=states[side].phase, result=checked(states[side],event); states[side]=result.state;
  if(mutant==='liveness' && event.type==='tick' && before==='disconnected' && event.now>=states[side].expiry && states[side].attemptUntil===null) assert.equal(states[side].phase,'closed','ヘ: 時計を進めれば満了する');
  if(event.type==='resume' && states[side].phase==='open' && before==='disconnected') { resumed=true; seen.add('successful resume'); }
  if(event.type==='tick' && before==='open' && states[side].phase==='disconnected' && silent.some(x=>x.has(result.effects.find(x=>x.type==='socket_close')?.socket))) seen.add('silent detected');
  if(event.type==='resume' && before==='open' && result.effects.some(x=>x.type==='transfer')) { replaced=true; seen.add('replacement'); }
  if(mutant==='omit-resend' && event.type==='resume' && before==='disconnected') { const index=result.effects.findLastIndex(x=>x.type==='frame'&&x.kind==='data'); if(index>=0) result.effects.splice(index,1); }
  if(mutant==='duplicate-resume' && event.type==='resume' && before==='disconnected' && states[side].phase==='open' && (states[1-side].record?.acked ?? 0)>0 && delivered[side].length) delivered[side].push(delivered[side][0]);
  mark(`cell ${before} + ${event.type}`);
  mark(`transition ${side?'server':'client'} ${before} + ${event.type} -> ${states[side].phase}`);
  for(const effect of result.effects) {
   if(effect.type==='result' && effect.result==='accepted') sent[side].push(event.payload);
   if(effect.type==='deliver') { delivered[side].push(effect.payload); if(resumed) seen.add('delivery after resume'); if(replaced) seen.add('delivery after replacement'); if(mutant==='delivery') delivered[side].push(effect.payload); }
   if(effect.type==='frame') queues[side].push(effect);
   if(effect.type==='end' && effect.reason!=='replaced') { if(effect.reason==='expired') seen.add('expiry'); if(effect.reason==='resume_rejected') seen.add('rejection'); if(!finalizing && (effect.reason==='closed_by_caller'||effect.reason==='peer_closed')) seen.add('close'); assert.equal(effect.count,pending.length,'通知と未 ack の集合の大きさ'); for(const id of pending) notices[side].add(id); }
  }
  for(let i=0;i<2;i++) assert.deepEqual(delivered[1-i],sent[i].slice(0,delivered[1-i].length),'イ: 配送は順の接頭');
  return result;
 }
 function frame(side,drop) { const f=queues[side][0]; if(!f || (side===1 && response?.socket===f.socket))return; queues[side].shift(); const silentDrop=silent[side].has(f.socket); drop ||= dead.has(f.socket)||silentDrop; mark(`frame ${drop?'lost':'delivered'} ${f.kind}`); if(drop) { uninterrupted=false; if(!silentDrop) dead.add(f.socket); } else if(f.kind==='identifier') { if(states[0].phase==='preparing' && states[0].socket===f.socket) { apply(0,{type:'ready',socket:f.socket}); queues[0].push({kind:'ready',socket:f.socket}); } } else if(f.kind==='ready') { if(states[1].phase==='preparing' && states[1].socket===f.socket) apply(1,{type:'ready',socket:f.socket}); } else apply(1-side,{type:f.kind==='close'?'peerClose':f.kind,seq:f.seq,payload:f.payload,socket:f.socket}); }
 for(const {op,side,variant} of list) {
  mark(`event ${['send','frame deliver','frame loss','loss','tick','attempt','request','response','request loss','response loss','close','replace','unknown resume','new connect'][op]}`);
  if(op===0) apply(side,{type:'send',payload:`${side}:${++nextId}:`+'x'.repeat(1+variant*8)});
  if(op===1 || op===2) frame(side,op===2);
  if(op===3 && states[side].socket!==null) { uninterrupted=false; dead.add(states[side].socket); apply(side,{type:'loss',socket:states[side].socket}); }
  if(op===4) { const s=states[side]; const values=[1,999,1000,9999,10000,600000]; apply(side,{type:'tick',now:s.now+values[variant]}); }
  if(op===5 && states[0].phase==='disconnected' && states[0].attemptUntil===null) { ++socket; apply(0,{type:'attempt',socket}); if(states[0].attemptUntil!==null) request={socket,seq:states[0].record.received}; }
  if(op===6 && request && !dead.has(request.socket)) {
   const req=request; request=null; const s=states[1], seq=s.record?.received ?? 0;
   const result=apply(1,{type:'resume',...req});
   const transfer=result.effects.find(x=>x.type==='transfer');
   if(transfer) { const result=successor(s,transfer.record,req.socket); states[1]=result.state; mark('replacement transfer'); for(const effect of result.effects.slice(1,mutant==='omit-resend'?-1:undefined)) queues[1].push(effect); }
   response={socket:req.socket,seq,rejected:result.effects.some(x=>x.type==='resume_reject')};
  }
  if(op===7 && response && !dead.has(response.socket)) { const res=response; response=null; apply(0,res.rejected?{type:'reject',socket:res.socket}:{type:'resume',socket:res.socket,seq:res.seq}); }
  if(op===8 && request) { dead.add(request.socket); request=null; mark('resume request lost'); }
  if(op===9 && response) { dead.add(response.socket); response=null; mark('resume response lost'); }
  if(op===10) apply(side,{type:'close'});
  if(op===11 && states[1].phase==='open') { uninterrupted=false; const s=states[1], result=apply(1,{type:'replace'}); const transfer=result.effects.find(x=>x.type==='transfer'); if(transfer) { const next=successor(s,transfer.record,++socket); states[1]=next.state; for(const effect of next.effects.slice(1)) queues[1].push(effect); mark('replacement transfer'); } }
  if(op===12) { const unknown={...initial('server'),phase:'closed',record:null,socket:null}; const r=checked(unknown,{type:'resume',socket:++socket,seq:0}); assert.equal(r.effects[0].reason,'unknown_identifier'); seen.add('rejection'); mark('unknown identifier rejected'); }
  if(op===13 && states.every(s=>s.phase==='closed')) { for(let i=0;i<2;i++) for(const id of sent[i]) assert(delivered[1-i].includes(id)||notices[i].has(id),`ト: 送信 ${id} が消失`); socket++; states=[initial('client','id',socket),initial('server','id',socket)]; queues=[[],[{kind:'identifier',socket}]]; sent=[[],[]]; delivered=[[],[]]; notices=[new Set(),new Set()]; request=null; response=null; uninterrupted=true; mark('new connection after close'); }
  if(op===14 && states[side].phase==='open') { const n=states[side].socket; silent[side].add(n); if(variant%2===0) silent[1-side].add(n); silentlyLost=true; mark('silent fault'); }
 }
 if(states.every(s=>s.phase!=='closed')) {
  if(request && !dead.has(request.socket) && states[0].phase==='disconnected') {
   const req=request; request=null; const before=states[1], seq=before.record?.received ?? 0;
   const result=apply(1,{type:'resume',...req}); const transfer=result.effects.find(x=>x.type==='transfer');
   if(transfer) { const next=successor(before,transfer.record,req.socket); states[1]=next.state; for(const effect of next.effects.slice(1,mutant==='omit-resend'?-1:undefined)) queues[1].push(effect); }
   response={socket:req.socket,seq,rejected:result.effects.some(x=>x.type==='resume_reject')};
  }
  if(response) { const res=response; response=null; if(!dead.has(res.socket)) apply(0,res.rejected?{type:'reject',socket:res.socket}:{type:'resume',socket:res.socket,seq:res.seq}); }
  while(queues[0].length||queues[1].length) { frame(0,false); frame(1,false); }
  if(states.some(s=>s.phase==='closed')) mark('settle failed: closed while draining');
  else if(states.some(s=>s.phase==='preparing')) { mark('settle failed: handshake lost'); for(let i=0;i<2;i++) if(states[i].socket!==null && dead.has(states[i].socket)) apply(i,{type:'loss',socket:states[i].socket}); }
  else if(states.every(s=>s.phase!=='closed')) {
   for(let i=0;i<2;i++) if(states[i].phase==='open' && silent[1-i].has(states[i].socket)) apply(i,{type:'tick',now:Math.max(states[i].now,states[i].lastReceived+states[i].lossAfter)});
   for(let i=0;i<2;i++) if(states[i].phase==='open' && dead.has(states[i].socket)) apply(i,{type:'loss',socket:states[i].socket});
   if(states[0].phase==='open' && (states[1].phase!=='open' || states[0].socket!==states[1].socket)) apply(0,{type:'loss',socket:states[0].socket});
   if(states[0].phase==='disconnected') {
    if(states[0].socket!==null && dead.has(states[0].socket)) apply(0,{type:'loss',socket:states[0].socket});
    if(states[0].phase==='disconnected' && states[0].attemptUntil!==null) apply(0,{type:'tick',now:states[0].attemptUntil});
    if(states[0].phase==='disconnected' && states[0].now<states[0].expiry) {
     const n=++socket; apply(0,{type:'attempt',socket:n});
     const before=states[1], seq=before.record?.received ?? 0;
     const result=apply(1,{type:'resume',socket:n,seq:states[0].record.received});
     const transfer=result.effects.find(x=>x.type==='transfer');
     if(transfer) { const next=successor(before,transfer.record,n); states[1]=next.state; for(const effect of next.effects.slice(1,mutant==='omit-resend'?-1:undefined)) queues[1].push(effect); }
     if(result.effects.some(x=>x.type==='resume_reject')) apply(0,{type:'reject',socket:n});
     else apply(0,{type:'resume',socket:n,seq});
    }
   }
   if(states.every(s=>s.phase==='open' && s.socket===states[0].socket)) {
    while(queues[0].length||queues[1].length) { frame(0,false); frame(1,false); }
    for(let i=0;i<2;i++) if(states[i].ackDue!==null) apply(i,{type:'tick',now:states[i].ackDue});
    while(queues[0].length||queues[1].length) { frame(0,false); frame(1,false); }
    for(let i=0;i<2;i++) { assert.deepEqual(delivered[1-i],sent[i],'イ: 再開後も欠けず順に配送'); assert.equal(states[i].record.pending.length,0,'未 ack が残っている'); }
    mark('settled and drained');
    if(silentlyLost && seen.has('silent detected') && resumed) mark('silent detected resumed and drained');
   } else mark(`settle failed: ${states.map(s=>s.phase).join('/')}`);
  }
 }
 finalizing=true;
 for(let i=0;i<2;i++) if(states[i].phase!=='closed') apply(i,{type:'close'});
 for(let i=0;i<2;i++) for(const id of sent[i]) assert(delivered[1-i].includes(id)||notices[i].has(id),`ト: 送信 ${id} が消失`);
 for(const key of seen) mark(`scenarios ${key}`);
 if(silentlyLost) mark('scenarios silent fault');
}
function fails(list, message) { try { scenario(list,false); return false; } catch(e) { return e.message===message; } }
function minimize(list, message) { for(let i=0;i<list.length;) { const candidate=list.toSpliced(i,1); if(fails(candidate,message)) { list=candidate; i=0; } else i++; } return list; }
if (!process.env.NET_ONLY) { fixedTimeoutAndClose(); unit(); fixedTests(); }
const start=Number(process.argv[2] || 1), count=Number(process.argv[3] || 100000);
for(let i=0;i<count;i++) { const seed=(start+i)>>>0 || 1, list=actions(seed); try { scenario(list); } catch(e) { const names=['send','frame deliver','frame loss','loss','tick','attempt','request','response','request loss','response loss','close','replace','unknown resume','new connect','silent fault']; console.error(`counterexample seed=${seed}\nminimized=${JSON.stringify(minimize(list,e.message).map(x=>({...x,op:names[x.op],side:x.side?'server':'client'})))}`); throw e; } }
console.log(`PASS seed=${start} scenarios=${count} checks=配送・通知・保持・終端・ack・期限・送信ID`);
for(const [phase,row] of Object.entries(descriptions)) for(const event of Object.keys(row)) { const key=`cell ${phase} + ${event}`; if(!coverage.has(key)) coverage.set(key,0); }
for(const [key,value] of [...coverage].sort(([a],[b])=>a.localeCompare(b))) console.log(`${key}: ${value}`);
