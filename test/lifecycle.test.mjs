import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Core, issueIdentifier } from '../dist/core.js';

const setup = (state, role='client') => {
  const c = new Core({role,identifier:'id',now:0,socket:'s'});
  if (state !== 'preparing') c.dispatch({type:'ready',socket:'s'});
  if (state === 'disconnected') c.dispatch({type:'loss',socket:'s'});
  if (state === 'closed') c.dispatch({type:'close'});
  return c;
};
const event = {
  ready:{type:'ready',socket:'s'}, send:{type:'send',data:'x'},
  ack:{type:'ack',socket:'s',seq:0}, data:{type:'data',socket:'s',seq:1,data:'x'},
  ping:{type:'ping',socket:'s'}, pong:{type:'pong',socket:'s'},
  loss:{type:'loss',socket:'s'}, attempt:{type:'attempt',socket:'n'},
  resume:{type:'resume',socket:'n',seq:0}, reject:{type:'reject',socket:'s'},
  expiry:{type:'expiry',now:600000}, close:{type:'close'}, peerClose:{type:'peerClose',socket:'s'},
  replace:{type:'replace'}, noIdentifier:{type:'noIdentifier',socket:'n'}, tick:{type:'tick',now:1000}
};
const effect = (r,t) => r.effects.find(x=>x.type===t);
test('server identifier uses injected timestamp and random bytes as UUIDv7',()=>{
  const id=issueIdentifier(1,n=>new Uint8Array(n).fill(255));
  assert.equal(id,'00000000-0001-7fff-bfff-ffffffffffff');
  assert.throws(()=>issueIdentifier(1,()=>new Uint8Array(1)));
});
const cases = {
  preparing: {
    ready:(c,r)=>{assert.equal(c.state,'open');assert.ok(effect(r,'connected'));},
    send:(c,r)=>{assert.equal(r.result,'not_open');assert.equal(c.state,'preparing');},
    ack:(c,r)=>{assert.equal(c.state,'closed');assert.equal(effect(r,'ended').reason,'protocol_error');},
    data:(c,r)=>assert.deepEqual(r.effects,[]), ping:(c,r)=>assert.deepEqual(r.effects,[]),
    pong:(c,r)=>assert.deepEqual(r.effects,[]),
    loss:(c,r)=>{assert.equal(c.state,'closed');assert.equal(effect(r,'ended').reason,'connect_failed');},
    attempt:(c,r)=>assert.deepEqual(r.effects,[]), resume:(c,r)=>assert.deepEqual(r.effects,[]),
    reject:(c,r)=>assert.deepEqual(r.effects,[]), expiry:(c,r)=>assert.deepEqual(r.effects,[]),
    close:(c,r)=>{assert.equal(c.state,'closed');assert.equal(r.result,'ok');assert.equal(effect(r,'ended').reason,'closed_by_caller');},
    peerClose:(c,r)=>assert.equal(effect(r,'ended').reason,'peer_closed'),
    replace:(c,r)=>assert.deepEqual(r.effects,[]), noIdentifier:(c,r)=>assert.deepEqual(r.effects,[]),
    tick:(c,r)=>{assert.equal(c.state,'preparing');assert.equal(c.now,1000);}
  },
  open: {
    ready:(c,r)=>assert.deepEqual(r.effects,[]),
    send:(c,r)=>{assert.equal(r.result,'accepted');assert.equal(r.seq,1);assert.equal(effect(r,'frame').frame.type,'data');},
    ack:(c,r)=>{assert.equal(c.state,'open');assert.deepEqual(r.effects,[]);},
    data:(c,r)=>{assert.equal(effect(r,'deliver').seq,1);assert.equal(c.record.received,1);},
    ping:(c,r)=>{assert.deepEqual(r.effects,[]);assert.equal(effect(c.dispatch({type:'tick',now:1000}),'frame').frame.type,'pong');},
    pong:(c,r)=>assert.deepEqual(r.effects,[]),
    loss:(c,r)=>{assert.equal(c.state,'disconnected');assert.ok(effect(r,'retry'));},
    attempt:(c,r)=>assert.deepEqual(r.effects,[]), resume:(c,r)=>assert.deepEqual(r.effects,[]),
    reject:(c,r)=>assert.deepEqual(r.effects,[]), expiry:(c,r)=>assert.equal(c.state,'open'),
    close:(c,r)=>{assert.equal(c.state,'closed');assert.equal(effect(r,'frame').frame.type,'close');},
    peerClose:(c,r)=>assert.equal(effect(r,'ended').reason,'peer_closed'),
    replace:(c,r)=>{assert.ok(effect(r,'transfer'));assert.equal(effect(r,'ended').reason,'replaced');assert.equal(c.state,'closed');},
    noIdentifier:(c,r)=>assert.deepEqual(r.effects,[]),
    tick:(c,r)=>{assert.equal(c.state,'open');assert.deepEqual(r.effects,[]);}
  },
  disconnected: {
    ready:(c,r)=>assert.deepEqual(r.effects,[]),
    send:(c,r)=>{assert.equal(r.result,'accepted');assert.deepEqual(r.effects,[]);},
    ack:(c,r)=>assert.deepEqual(r.effects,[]), data:(c,r)=>assert.deepEqual(r.effects,[]),
    ping:(c,r)=>assert.deepEqual(r.effects,[]), pong:(c,r)=>assert.deepEqual(r.effects,[]),
    loss:(c,r)=>assert.ok(effect(r,'retry')),
    attempt:(c,r)=>{assert.deepEqual(r.effects,[]);assert.equal(c.socket,'n');},
    resume:(c,r)=>{assert.equal(c.state,'disconnected');assert.deepEqual(r.effects,[]);},
    reject:(c,r)=>assert.equal(effect(r,'ended').reason,'resume_rejected'),
    expiry:(c,r)=>{assert.equal(c.state,'closed');assert.equal(effect(r,'ended').reason,'expired');},
    close:(c,r)=>{assert.equal(c.state,'closed');assert.equal(r.result,'ok');},
    peerClose:(c,r)=>assert.equal(effect(r,'ended').reason,'peer_closed'),
    replace:(c,r)=>assert.deepEqual(r.effects,[]), noIdentifier:(c,r)=>assert.deepEqual(r.effects,[]),
    tick:(c,r)=>{assert.equal(c.state,'disconnected');assert.deepEqual(r.effects,[]);}
  },
  closed: {
    ready:(c,r)=>assert.deepEqual(r.effects,[]), send:(c,r)=>assert.equal(r.result,'closed'),
    ack:(c,r)=>assert.deepEqual(r.effects,[]), data:(c,r)=>assert.deepEqual(r.effects,[]),
    ping:(c,r)=>assert.deepEqual(r.effects,[]), pong:(c,r)=>assert.deepEqual(r.effects,[]),
    loss:(c,r)=>assert.deepEqual(r.effects,[]), attempt:(c,r)=>assert.deepEqual(r.effects,[]),
    resume:(c,r)=>assert.deepEqual(r.effects,[]), reject:(c,r)=>assert.deepEqual(r.effects,[]),
    expiry:(c,r)=>assert.deepEqual(r.effects,[]), close:(c,r)=>{assert.equal(r.result,'ok');assert.deepEqual(r.effects,[]);},
    peerClose:(c,r)=>assert.deepEqual(r.effects,[]), replace:(c,r)=>assert.deepEqual(r.effects,[]),
    noIdentifier:(c,r)=>assert.deepEqual(r.effects,[]), tick:(c,r)=>{assert.equal(c.now,1000);assert.deepEqual(r.effects,[]);}
  }
};
for (const [state,events] of Object.entries(cases)) for (const [name,check] of Object.entries(events)) {
  test(`magnetite.lifecycle.${state}.${name}`,()=>{
    const c=setup(state);
    if(state==='disconnected'&&['loss','reject','peerClose'].includes(name)){
      c.dispatch({type:'attempt',socket:'n'});
      check(c,c.dispatch({...event[name],socket:'n'}));
    } else check(c,c.dispatch(event[name]));
  });
}

test('server noIdentifier in every state creates a new connection',()=>{
  for(const state of Object.keys(cases)) assert.equal(effect(setup(state,'server').dispatch(event.noIdentifier),'newConnection').socket,'n');
});
test('open server resume validates and transfers',()=>{
  const c=setup('open','server'); c.dispatch({type:'send',data:'x'});
  assert.equal(effect(c.dispatch({type:'resume',socket:'n',seq:2}),'reject').reason,'invalid_sequence');
  const r=c.dispatch({type:'resume',socket:'n',seq:0});
  assert.equal(effect(r,'transfer').record.pending.length,1);assert.equal(effect(r,'ended').reason,'replaced');
});
test('client resume retransmits only remaining messages',()=>{
  const c=setup('open');c.dispatch({type:'send',data:1});c.dispatch({type:'send',data:2});
  c.dispatch({type:'loss',socket:'s'});c.dispatch({type:'attempt',socket:'n'});
  const r=c.dispatch({type:'resume',socket:'n',seq:1});
  assert.equal(c.state,'open');assert.deepEqual(r.effects.filter(x=>x.type==='frame').map(x=>x.frame.seq),[2]);
});
test('exact attempt timeout precedes reply, expiry defers active attempt',()=>{
  const c=new Core({role:'client',identifier:'i',now:0,socket:'s',expiryAfter:5000});
  c.dispatch({type:'ready',socket:'s'});c.dispatch({type:'loss',socket:'s'});c.dispatch({type:'attempt',socket:'n'});
  c.dispatch({type:'expiry',now:5000});assert.equal(c.state,'disconnected');
  const r=c.dispatch({type:'tick',now:10000});assert.equal(effect(r,'ended').reason,'expired');
});
test('disconnected expiry advances time to the deadline',()=>{
  const c=setup('disconnected');const r=c.dispatch({type:'expiry',now:0});
  assert.equal(c.now,600000);assert.equal(effect(r,'ended').reason,'expired');
});
test('ack and pong deadlines stay fixed; loss wins over both',()=>{
  const c=setup('open');c.dispatch(event.data);c.dispatch(event.ping);
  c.dispatch({type:'tick',now:500});c.dispatch(event.data);c.dispatch(event.ping);
  const r=c.dispatch({type:'tick',now:1000});assert.deepEqual(r.effects.filter(x=>x.type==='frame').map(x=>[x.frame.type,x.at]),[['ack',1000],['pong',1000]]);
  c.dispatch({type:'data',socket:'s',seq:2,data:2});c.dispatch(event.ping);
  const lost=c.dispatch({type:'tick',now:31000});assert.equal(c.state,'disconnected');assert.equal(lost.effects.filter(x=>x.type==='frame').length,0);
});
test('ping once, receipt resets, and loss at threshold takes precedence',()=>{
  const c=setup('open');assert.equal(effect(c.dispatch({type:'tick',now:15000}),'frame').frame.type,'ping');
  assert.deepEqual(c.dispatch({type:'tick',now:16000}).effects,[]);c.dispatch(event.pong);
  assert.equal(effect(c.dispatch({type:'tick',now:31000}),'frame').frame.type,'ping');
  const r=c.dispatch({type:'tick',now:46000});assert.equal(c.state,'disconnected');assert.ok(effect(r,'closeSocket'));assert.equal(effect(r,'frame'),undefined);
});
test('capacity accounts for actual JSON UTF-8 and releases on cumulative ack',()=>{
  const frame=JSON.stringify({type:'data',seq:1,data:'é'});
  const c=new Core({role:'client',identifier:'i',now:0,socket:'s',maxCount:1,maxBytes:new TextEncoder().encode(frame).length});
  c.dispatch({type:'ready',socket:'s'});assert.equal(c.dispatch({type:'send',data:'é'}).result,'accepted');
  assert.equal(c.dispatch({type:'send',data:'x'}).result,'limit');c.dispatch({type:'ack',socket:'s',seq:1});assert.equal(c.dispatch({type:'send',data:'x'}).result,'accepted');
});
test('message count limit independently rejects at the boundary',()=>{
  const c=new Core({role:'client',identifier:'i',now:0,socket:'s',maxCount:1,maxBytes:10000});
  c.dispatch({type:'ready',socket:'s'});
  assert.equal(c.dispatch({type:'send',data:'x'}).result,'accepted');
  assert.equal(c.dispatch({type:'send',data:'x'}).result,'limit');
});
test('sequence validation, duplicate suppression, and stale socket isolation',()=>{
  const c=setup('open');assert.deepEqual(c.dispatch({type:'data',socket:'old',seq:1,data:'x'}).effects,[]);
  assert.ok(effect(c.dispatch(event.data),'deliver'));assert.equal(effect(c.dispatch(event.data),'deliver'),undefined);
  assert.equal(effect(c.dispatch({type:'data',socket:'s',seq:3,data:'x'}),'ended').reason,'protocol_error');
});
