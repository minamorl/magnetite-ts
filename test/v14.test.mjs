import {test} from 'node:test';
import assert from 'node:assert/strict';
import {Core} from '../dist/core.js';

function opened(role='client',options={}) {
  const c=new Core({role,identifier:'id',now:0,socket:'s',...options});
  c.dispatch({type:'ready',socket:'s'});return c;
}
test('magnetite.lifecycle.retired_socket: loss retires socket before later events',()=>{
  const c=opened();c.dispatch({type:'send',data:'pending'});
  assert.deepEqual(c.dispatch({type:'loss',socket:'s'}).effects,[{type:'retry'}]);
  assert.equal(c.socket,undefined);
  for(const event of [{type:'loss'},{type:'peerClose'},{type:'reject'},{type:'resume',seq:0},{type:'ack',seq:1},{type:'data',seq:1,data:'x'},{type:'ping'},{type:'pong'}]){
    assert.deepEqual(c.dispatch({...event,socket:'s'}).effects,[]);
    assert.equal(c.state,'disconnected');assert.equal(c.record.pending.length,1);
  }
});
test('magnetite.lifecycle.open.tick: silence retires socket',()=>{
  const c=opened();c.dispatch({type:'tick',now:30000});
  assert.equal(c.socket,undefined);
  assert.deepEqual(c.dispatch({type:'loss',socket:'s'}).effects,[]);
  assert.deepEqual(c.dispatch({type:'peerClose',socket:'s'}).effects,[]);
});
test('magnetite.lifecycle.disconnected.expiry: timeout closes socket before expired',()=>{
  const c=opened('client',{expiryAfter:10000});
  c.dispatch({type:'loss',socket:'s'});c.dispatch({type:'attempt',socket:'n'});
  const r=c.dispatch({type:'expiry',now:10000});
  assert.deepEqual(r.effects,[{type:'closeSocket',socket:'n'},{type:'ended',reason:'expired',unacked:0}]);
  assert.equal(c.socket,undefined);
});
test('magnetite.lifecycle.disconnected.tick: timeout closes socket before retry',()=>{
  const c=opened();c.dispatch({type:'loss',socket:'s'});c.dispatch({type:'attempt',socket:'n'});
  assert.deepEqual(c.dispatch({type:'tick',now:10000}).effects,[{type:'closeSocket',socket:'n'},{type:'retry'}]);
  assert.equal(c.socket,undefined);
  assert.deepEqual(c.dispatch({type:'reject',socket:'n'}).effects,[]);
});
test('magnetite.lifecycle.retired_socket: closed server ignores old socket but rejects new resume',()=>{
  const c=opened('server');c.dispatch({type:'close'});
  assert.deepEqual(c.dispatch({type:'resume',socket:'s',seq:0}).effects,[]);
  assert.deepEqual(c.dispatch({type:'resume',socket:'new',seq:0}).effects,[{type:'reject',socket:'new',reason:'unknown_identifier'}]);
});
test('magnetite.lifecycle.disconnected.expiry: active attempt before timeout survives expiry',()=>{
  const c=opened('client',{expiryAfter:5000});
  c.dispatch({type:'loss',socket:'s'});c.dispatch({type:'attempt',socket:'n'});
  assert.deepEqual(c.dispatch({type:'expiry',now:5000}).effects,[]);
  assert.equal(c.state,'disconnected');assert.equal(c.socket,'n');
  assert.deepEqual(c.dispatch({type:'expiry',now:10000}).effects,[{type:'closeSocket',socket:'n'},{type:'ended',reason:'expired',unacked:0}]);
});
test('magnetite.lifecycle.disconnected.close: lost socket is not a close frame destination',()=>{
  const c=opened();c.dispatch({type:'loss',socket:'s'});
  assert.deepEqual(c.dispatch({type:'close'}).effects,[{type:'ended',reason:'closed_by_caller',unacked:0}]);
});
test('magnetite.model.close.reservations: terminal transition clears both deadlines',()=>{
  const c=opened();c.dispatch({type:'data',socket:'s',seq:1,data:'x'});c.dispatch({type:'ping',socket:'s'});
  assert.equal(c.ackAt,1000);assert.equal(c.pongAt,1000);
  c.dispatch({type:'close'});
  assert.equal(c.socket,undefined);assert.equal(c.ackAt,undefined);assert.equal(c.pongAt,undefined);
  assert.deepEqual(c.dispatch({type:'tick',now:1000}).effects,[]);
});
for(const end of ['peerClose','replace','protocol_error']){
  test(`magnetite.model.close.reservations: ${end} clears deadlines`,()=>{
    const c=opened();c.dispatch({type:'data',socket:'s',seq:1,data:'x'});c.dispatch({type:'ping',socket:'s'});
    if(end==='peerClose')c.dispatch({type:'peerClose',socket:'s'});
    else if(end==='replace')c.dispatch({type:'replace'});
    else c.dispatch({type:'ack',socket:'s',seq:-1});
    assert.equal(c.state,'closed');assert.equal(c.ackAt,undefined);assert.equal(c.pongAt,undefined);
    assert.deepEqual(c.dispatch({type:'tick',now:1000}).effects,[]);
  });
}
