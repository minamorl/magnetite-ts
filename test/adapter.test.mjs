import {test} from 'node:test';
import assert from 'node:assert/strict';
import {Client,Server} from '../dist/index.js';

class Clock {
  time=0; serial=0; jobs=new Map();
  now=()=>this.time;
  setTimeout=(fn,delay)=>{const id=++this.serial;this.jobs.set(id,{at:this.time+delay,fn});return id;};
  clearTimeout=id=>this.jobs.delete(id);
  advance(ms){const end=this.time+ms;for(let n=0;n<100000;n++){const next=[...this.jobs].sort((a,b)=>a[1].at-b[1].at||a[0]-b[0])[0];if(!next||next[1].at>end)break;this.jobs.delete(next[0]);this.time=next[1].at;next[1].fn();}this.time=end;}
}
class FakeSocket {
  readyState=0; handlers=new Map(); peer; blocked=false; isolated=false; sent=[]; sentAt=[];
  constructor(clock){this.clock=clock;this.createdAt=clock.now();}
  addEventListener(type,fn){const list=this.handlers.get(type)??[];list.push(fn);this.handlers.set(type,list);}
  emit(type,event={}){for(const f of this.handlers.get(type)??[])f(event);}
  send(data){if(this.readyState!==1)throw Error('not open');this.sent.push(JSON.parse(data));this.sentAt.push(this.clock.now());if(!this.blocked)this.clock.setTimeout(()=>{if(this.peer.readyState===1)this.peer.emit('message',{data});},0);}
  close(){if(this.readyState>=2)return;this.readyState=2;this.clock.setTimeout(()=>{this.readyState=3;this.emit('close');if(!this.isolated&&this.peer.readyState<2){this.peer.readyState=3;this.peer.emit('close');}},0);}
}
function preparing() {
  const clock=new Clock(),a=new FakeSocket(clock),b=new FakeSocket(clock),ends=[];
  a.peer=b;b.peer=a;a.readyState=b.readyState=1;
  const client=new Client('ws://example',{clock,socketFactory:()=>a,onEnd:n=>ends.push(n)});
  client.connect();a.emit('open');clock.advance(0);
  return {clock,a,b,client,ends};
}
for(const frame of [{type:'data',seq:1,data:'x'},{type:'ping'},{type:'pong'},{type:'reject',reason:'unknown_identifier'}]){
  test(`injected sockets: preparing ignores ${frame.type}`,()=>{
    const f=preparing();f.b.send(JSON.stringify(frame));f.clock.advance(0);
    assert.equal(f.client.state,'preparing');assert.deepEqual(f.ends,[]);f.client.close();f.clock.advance(0);
  });
}
test('injected sockets: ack while preparing is a protocol error',()=>{
  const f=preparing();f.b.send(JSON.stringify({type:'ack',seq:0}));f.clock.advance(0);
  assert.deepEqual(f.ends,[{reason:'protocol_error',unacked:0}]);
});
test('injected sockets: loss while preparing reports connect_failed',()=>{
  const f=preparing();f.a.close();f.clock.advance(0);assert.deepEqual(f.ends,[{reason:'connect_failed',unacked:0}]);
});
function fixture(extra={}) {
  const clock=new Clock(), serverReceived=[],clientReceived=[], ends=[], states=[],connections=[],serverEnds=[],pairs=[];let disabled=false,stalled=false,dropReply=false,stateAction=()=>{};
  const timing={clock,tickInterval:5,pingAfter:40,lossAfter:80,expiryAfter:500,...extra};
  const makeServer=()=>new Server({...timing,randomBytes:n=>new Uint8Array(n).fill(7),onConnection:c=>{connections.push(c);c.setCallbacks({onReceive:(data)=>serverReceived.push(data),onEnd:n=>serverEnds.push(n)});}});
  let server=makeServer();
  const socketFactory=()=>{const a=new FakeSocket(clock),b=new FakeSocket(clock);a.peer=b;b.peer=a;pairs.push([a,b]);const send=b.send.bind(b);b.send=data=>{const frame=JSON.parse(data);if(dropReply&&frame.type==='welcome'&&frame.resumed){dropReply=false;return;}send(data);};server.accept(b);clock.setTimeout(()=>{if(stalled)return;if(disabled)a.close();else{a.readyState=1;b.readyState=1;a.emit('open');}},0);return a;};
  const client=new Client('ws://example',{...timing,socketFactory,retryInitial:extra.retryInitial??10,retryMax:extra.retryMax??20,random:extra.random??(()=>0),onReceive:data=>clientReceived.push(data),onState:s=>{states.push(s);stateAction(s);},onEnd:n=>ends.push(n)});
  client.connect();clock.advance(0);
  return {clock,client,server,connections,pairs,serverReceived,clientReceived,ends,states,serverEnds,restart:()=>{server=makeServer();},disable:()=>{disabled=true;},stall:()=>{stalled=true;},dropResumeReply:()=>{dropReply=true;},onState:fn=>{stateAction=fn;}};
}
test('injected sockets: handshake, bidirectional frames, ack and close',()=>{
  const f=fixture();assert.equal(f.client.state,'open');assert.equal(f.connections.length,1);
  assert.equal(f.client.send('up').result,'accepted');f.connections[0].send('down');f.clock.advance(1);
  assert.deepEqual(f.serverReceived,['up']);assert.deepEqual(f.clientReceived,['down']);
  f.client.close();f.clock.advance(0);assert.equal(f.ends[0].reason,'closed_by_caller');
});
test('injected sockets: lost socket ignored and retained send resumes',()=>{
  const f=fixture();f.pairs[0][0].close();f.clock.advance(0);assert.equal(f.client.state,'disconnected');
  assert.equal(f.client.send('held').result,'accepted');f.clock.advance(20);
  assert.equal(f.client.state,'open');assert.deepEqual(f.serverReceived,['held']);
  f.pairs[0][0].emit('close');f.pairs[0][0].emit('message',{data:'{"type":"reject"}'});
  assert.equal(f.client.state,'open');f.client.close();f.clock.advance(0);
});
for(const frame of [{type:'ack',seq:99},{type:'data',seq:9,data:'stale'},{type:'ping'},{type:'pong'},{type:'reject'},{type:'close'},{type:'welcome',identifier:'old',seq:0,resumed:true}]) {
  test(`injected sockets: retired socket ignores ${frame.type}`,()=>{
    const f=fixture();const old=f.pairs[0][0];old.close();f.clock.advance(20);
    assert.equal(f.client.state,'open');old.emit('message',{data:JSON.stringify(frame)});
    assert.equal(f.client.state,'open');assert.deepEqual(f.clientReceived,[]);assert.deepEqual(f.ends,[]);
    f.client.close();f.clock.advance(0);
  });
}
test('injected sockets: callback sends follow retransmissions in sequence',()=>{
  const f=fixture();f.pairs[0][0].close();f.clock.advance(0);
  f.client.send('held');f.onState(state=>{if(state==='open')f.client.send('new');});
  f.clock.advance(20);assert.deepEqual(f.serverReceived,['held','new']);
  assert.equal(f.client.state,'open');f.client.close();f.clock.advance(0);
});
test('injected sockets: silent loss causes ping and reconnect',()=>{
  const f=fixture();f.pairs[0][0].blocked=true;f.pairs[0][1].blocked=true;
  f.clock.advance(110);assert.equal(f.client.state,'open');assert.ok(f.pairs.length>=2);
  assert.ok(f.states.includes('disconnected'));f.client.close();f.clock.advance(0);
});
test('injected sockets: restarted server rejects and reports unacknowledged count',()=>{
  const f=fixture();f.restart();f.pairs[0][0].close();f.clock.advance(0);
  assert.equal(f.client.send('held').result,'accepted');f.clock.advance(20);
  assert.deepEqual(f.ends,[{reason:'resume_rejected',unacked:1}]);
});
test('injected sockets: duplicate is acked without duplicate delivery',()=>{
  const f=fixture({pingAfter:2000,lossAfter:4000});f.client.send('x');f.clock.advance(0);
  f.pairs[0][0].send(JSON.stringify({type:'data',seq:1,data:'x'}));f.clock.advance(1001);
  assert.deepEqual(f.serverReceived,['x']);
  assert.ok(f.pairs[0][1].sent.some(x=>x.type==='ack'&&x.seq===1));f.client.close();f.clock.advance(0);
});
test('injected sockets: invalid sequence closes and reports pending',()=>{
  const f=fixture();f.client.send('pending');f.pairs[0][1].send(JSON.stringify({type:'ack',seq:999}));f.clock.advance(0);
  assert.deepEqual(f.ends,[{reason:'protocol_error',unacked:1}]);
});
test('injected sockets: peer close reports the pending count once',()=>{
  const f=fixture();f.client.send('pending');f.connections[0].close();f.clock.advance(0);
  assert.deepEqual(f.ends,[{reason:'peer_closed',unacked:1}]);
  f.pairs[0][0].emit('close');assert.equal(f.ends.length,1);
});
test('injected sockets: ping and pong leave delivery sequence unchanged',()=>{
  const f=fixture({lossAfter:2000});f.clock.advance(45);
  assert.ok(f.pairs[0][0].sent.some(x=>x.type==='ping'));
  f.clock.advance(1000);
  assert.ok(f.pairs[0][0].sent.some(x=>x.type==='pong'));
  assert.equal(f.client.send('first').seq,1);f.clock.advance(0);
  assert.deepEqual(f.serverReceived,['first']);f.client.close();f.clock.advance(0);
});
test('injected sockets: initial factory failure reports connect_failed',()=>{
  const clock=new Clock(),ends=[];
  const c=new Client('ws://example',{clock,socketFactory:()=>{throw Error('failure');},onEnd:n=>ends.push(n)});
  c.connect();assert.deepEqual(ends,[{reason:'connect_failed',unacked:0}]);assert.equal(c.state,'closed');
});
test('injected sockets: retry continues until expiry and reports each retained send',()=>{
  const f=fixture();f.disable();f.pairs[0][0].close();f.clock.advance(0);
  assert.equal(f.client.send('pending').result,'accepted');f.clock.advance(510);
  assert.ok(f.pairs.length>2);assert.deepEqual(f.ends,[{reason:'expired',unacked:1}]);
});
test('injected sockets: retry delay doubles and respects its configured cap',()=>{
  const f=fixture();f.disable();f.pairs[0][0].close();f.clock.advance(0);f.clock.advance(50);
  assert.deepEqual(f.pairs.slice(1,4).map(pair=>pair[0].createdAt),[10,30,50]);
  f.client.close();f.clock.advance(0);
});
test('injected sockets: a stalled attempt is closed at ten seconds after expiry',()=>{
  const f=fixture();f.stall();f.pairs[0][0].close();f.clock.advance(0);
  f.client.send('pending');f.clock.advance(10009);assert.equal(f.client.state,'disconnected');
  f.clock.advance(1);assert.deepEqual(f.ends,[{reason:'expired',unacked:1}]);
  assert.equal(f.pairs[1][0].readyState,3);
});
test('injected sockets: lost resume reply replaces server connection and preserves messages',()=>{
  const f=fixture({tickInterval:50,pingAfter:20000,lossAfter:40000,expiryAfter:30000});
  f.pairs[0][1].blocked=true;f.connections[0].send('server-held');
  f.dropResumeReply();f.pairs[0][0].close();f.clock.advance(0);
  assert.equal(f.client.send('held').result,'accepted');
  f.clock.advance(10);f.pairs[1][0].isolated=true;f.clock.advance(10040);
  assert.equal(f.client.state,'open');assert.deepEqual(f.serverReceived,['held']);assert.deepEqual(f.clientReceived,['server-held']);
  assert.ok(f.serverEnds.some(x=>x.reason==='replaced'&&x.unacked===undefined));
  f.client.close();f.clock.advance(0);
});
test('injected sockets: ack and pong meet one-second deadlines between periodic ticks',()=>{
  const f=fixture({tickInterval:250,pingAfter:5000,lossAfter:10000});
  f.clock.advance(10);f.client.send('x');f.pairs[0][0].send(JSON.stringify({type:'ping'}));f.clock.advance(999);
  assert.equal(f.pairs[0][1].sent.some(x=>x.type==='ack'||x.type==='pong'),false);
  f.clock.advance(1);
  for(const type of ['ack','pong']){
    const i=f.pairs[0][1].sent.findIndex(x=>x.type===type);
    assert.notEqual(i,-1);assert.equal(f.pairs[0][1].sentAt[i],1010);
  }
  f.client.close();f.clock.advance(0);
});
test('injected sockets: ping and silent loss fire at configured deadlines between periodic ticks',()=>{
  const f=fixture({tickInterval:250,pingAfter:1050,lossAfter:2100,expiryAfter:5000});
  f.pairs[0][0].blocked=true;f.pairs[0][1].blocked=true;
  f.clock.advance(1049);assert.equal(f.pairs[0][0].sent.some(x=>x.type==='ping'),false);
  f.clock.advance(1);
  const i=f.pairs[0][0].sent.findIndex(x=>x.type==='ping');
  assert.notEqual(i,-1);assert.equal(f.pairs[0][0].sentAt[i],1050);
  f.clock.advance(1049);assert.equal(f.client.state,'open');
  f.clock.advance(1);assert.equal(f.client.state,'disconnected');f.client.close();f.clock.advance(0);
});
test('injected sockets: cumulative ack frees the configured send window',()=>{
  const f=fixture({maxCount:1,pingAfter:5000,lossAfter:10000});
  assert.equal(f.client.send('one').result,'accepted');assert.equal(f.client.send('two').result,'limit');
  f.clock.advance(1000);assert.equal(f.client.send('two').result,'accepted');
  f.clock.advance(0);assert.deepEqual(f.serverReceived,['one','two']);f.client.close();f.clock.advance(0);
});
test('injected sockets: invalid server resume preserves the active connection',()=>{
  const f=fixture(),a=new FakeSocket(f.clock),b=new FakeSocket(f.clock),messages=[];
  a.peer=b;b.peer=a;a.readyState=b.readyState=1;f.server.accept(b);
  a.addEventListener('message',event=>messages.push(JSON.parse(event.data)));
  a.send(JSON.stringify({type:'hello',identifier:f.connections[0].identifier,seq:99}));f.clock.advance(0);
  assert.deepEqual(messages,[{type:'reject',reason:'invalid_sequence'}]);
  assert.equal(f.connections[0].state,'open');f.client.send('still open');f.clock.advance(0);
  assert.deepEqual(f.serverReceived,['still open']);f.client.close();f.clock.advance(0);
});
test('injected sockets: resume of active server replaces old connection',()=>{
  const f=fixture(),a=new FakeSocket(f.clock),b=new FakeSocket(f.clock),messages=[];
  a.peer=b;b.peer=a;a.readyState=b.readyState=1;f.server.accept(b);
  a.addEventListener('message',event=>messages.push(JSON.parse(event.data)));
  a.send(JSON.stringify({type:'hello',identifier:f.connections[0].identifier,seq:0}));f.clock.advance(0);
  assert.equal(f.connections[0].state,'closed');assert.equal(f.connections[1].state,'open');
  assert.equal(messages[0].type,'welcome');assert.equal(messages[0].resumed,true);
  assert.ok(f.serverEnds.some(n=>n.reason==='replaced'));a.close();f.client.close();f.clock.advance(0);
});
test('v1.5 server waits for ready and refuses sends until ready',()=>{
  const clock=new Clock(),a=new FakeSocket(clock),b=new FakeSocket(clock),connections=[];
  a.peer=b;b.peer=a;a.readyState=b.readyState=1;
  const server=new Server({clock,randomBytes:n=>new Uint8Array(n),onConnection:c=>connections.push(c)});
  server.accept(b);a.send(JSON.stringify({type:'hello'}));clock.advance(0);
  assert.equal(connections[0].state,'preparing');assert.equal(connections[0].send('x').result,'not_open');
  a.send(JSON.stringify({type:'ready'}));clock.advance(0);assert.equal(connections[0].state,'open');
  connections[0].close();clock.advance(0);
});
test('v1.5 client sends ready before opening',()=>{
  const f=preparing();f.b.send(JSON.stringify({type:'welcome',identifier:'id',seq:0,resumed:false}));
  f.clock.advance(0);assert.equal(f.client.state,'open');assert.equal(f.a.sent.at(-1).type,'ready');
  f.client.close();f.clock.advance(0);
});
test('v1.5 client deadline starts at request and not socket creation',()=>{
  const clock=new Clock(),a=new FakeSocket(clock),b=new FakeSocket(clock),ends=[];
  a.peer=b;b.peer=a;
  const c=new Client('ws://example',{clock,lossAfter:80,tickInterval:5,socketFactory:()=>a,onEnd:n=>ends.push(n)});
  c.connect();clock.advance(100);assert.equal(c.state,'preparing');
  a.readyState=b.readyState=1;a.emit('open');clock.advance(79);assert.equal(c.state,'preparing');
  clock.advance(1);assert.deepEqual(ends,[{reason:'connect_failed',unacked:0}]);
});
test('v1.5 server preparation deadline does not move on handshake frames',()=>{
  const clock=new Clock(),a=new FakeSocket(clock),b=new FakeSocket(clock),connections=[],ends=[];
  a.peer=b;b.peer=a;a.readyState=b.readyState=1;
  const server=new Server({clock,lossAfter:80,tickInterval:5,randomBytes:n=>new Uint8Array(n),onConnection:c=>{connections.push(c);c.setCallbacks({onEnd:n=>ends.push(n)});}});
  server.accept(b);a.send(JSON.stringify({type:'hello'}));clock.advance(0);
  clock.advance(70);a.send(JSON.stringify({type:'ping'}));clock.advance(10);
  assert.equal(connections[0].state,'closed');assert.deepEqual(ends,[{reason:'connect_failed',unacked:0}]);
});
test('v1.5 resume reply precedes replaced notification and successor effects',()=>{
  const f=fixture(),a=new FakeSocket(f.clock),b=new FakeSocket(f.clock),order=[];
  a.peer=b;b.peer=a;a.readyState=b.readyState=1;f.server.accept(b);
  const send=b.send.bind(b);b.send=data=>{order.push(JSON.parse(data).type);send(data);};
  f.connections[0].setCallbacks({onEnd:()=>order.push('ended')});
  a.send(JSON.stringify({type:'hello',identifier:f.connections[0].identifier,seq:0}));f.clock.advance(0);
  assert.deepEqual(order.slice(0,2),['welcome','ended']);
  f.client.close();a.close();f.clock.advance(0);
});
test('v1.5 preparing server ignores resume instead of accepting it',()=>{
  const clock=new Clock(),a=new FakeSocket(clock),b=new FakeSocket(clock),connections=[];
  a.peer=b;b.peer=a;a.readyState=b.readyState=1;
  const server=new Server({clock,randomBytes:n=>new Uint8Array(n),onConnection:c=>connections.push(c)});
  server.accept(b);a.send(JSON.stringify({type:'hello'}));clock.advance(0);
  const x=new FakeSocket(clock),y=new FakeSocket(clock),messages=[];x.peer=y;y.peer=x;x.readyState=y.readyState=1;
  x.addEventListener('message',e=>messages.push(JSON.parse(e.data)));server.accept(y);
  x.send(JSON.stringify({type:'hello',identifier:connections[0].identifier,seq:0}));clock.advance(0);
  assert.deepEqual(messages,[]);assert.equal(connections[0].state,'preparing');connections[0].close();clock.advance(0);
});
test('v1.5 retry uses injected nonzero jitter without passing cap',()=>{
  const f=fixture({random:()=>1});f.disable();f.pairs[0][0].close();f.clock.advance(0);f.clock.advance(55);
  assert.deepEqual(f.pairs.slice(1,4).map(pair=>pair[0].createdAt),[11,31,51]);
  f.client.close();f.clock.advance(0);
});
test('v1.5 retry never exceeds ten seconds even when configured cap is larger',()=>{
  const f=fixture({retryInitial:8000,retryMax:20000,expiryAfter:100000,tickInterval:100,random:()=>1});
  f.disable();f.pairs[0][0].close();f.clock.advance(0);f.clock.advance(30000);
  assert.deepEqual(f.pairs.slice(1,4).map(pair=>pair[0].createdAt),[8800,18800,28800]);
  f.client.close();f.clock.advance(0);
});
test('v1.5 invalid resume on current socket only rejects and refreshes liveness',()=>{
  const f=fixture({tickInterval:5,pingAfter:40,lossAfter:200});
  f.clock.advance(30);const before=f.serverEnds.length;
  f.pairs[0][0].send(JSON.stringify({type:'hello',identifier:f.connections[0].identifier,seq:99}));f.clock.advance(0);
  assert.equal(f.connections[0].state,'open');assert.equal(f.serverEnds.length,before);
  assert.equal(f.pairs[0][1].sent.at(-1).type,'reject');f.clock.advance(11);
  assert.equal(f.pairs[0][1].sent.some(x=>x.type==='ping'),false);
  f.client.close();f.clock.advance(0);
});
test('v1.5 successful resume on current socket replies before replacement',()=>{
  const f=fixture(),order=[],serverSocket=f.pairs[0][1],send=serverSocket.send.bind(serverSocket);
  serverSocket.send=data=>{order.push(JSON.parse(data).type);send(data);};
  f.connections[0].setCallbacks({onEnd:()=>order.push('ended')});
  f.pairs[0][0].send(JSON.stringify({type:'hello',identifier:f.connections[0].identifier,seq:0}));
  f.clock.advance(0);assert.deepEqual(order.slice(0,2),['welcome','ended']);
  assert.equal(serverSocket.readyState,1);assert.equal(f.connections[1].state,'open');
  f.client.close();f.clock.advance(0);
});
