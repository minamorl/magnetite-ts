import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createServer,connect as tcpConnect} from 'node:net';
import {WebSocketServer} from 'ws';
import {Client,Server} from '../dist/index.js';

const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(predicate,label){const end=Date.now()+4000;while(!predicate()){if(Date.now()>end)throw Error(`timeout: ${label}`);await delay(5);}}
async function listen(server){await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));return server.address().port;}
async function wsFixture() {
  const wss=new WebSocketServer({port:0,host:'127.0.0.1'});
  await new Promise(resolve=>wss.once('listening',resolve));
  const port=wss.address().port,connections=[],received=[],clientReceived=[],ends=[],states=[],serverStates=[];
  const options={pingAfter:200,lossAfter:1500,tickInterval:10,expiryAfter:3500};
  const makeServer=()=>new Server({...options,onConnection:c=>{connections.push(c);c.setCallbacks({onReceive:data=>received.push(data),onState:state=>serverStates.push(state)});}});
  let adapter=makeServer();
  wss.on('connection',socket=>adapter.accept(socket));
  const createClient=(url=`ws://127.0.0.1:${port}`)=>new Client(url,{...options,retryInitial:30,retryMax:60,random:()=>1,onReceive:data=>clientReceived.push(data),onEnd:n=>ends.push(n),onState:s=>states.push(s)});
  const cleanup=async()=>{for(const socket of wss.clients)socket.terminate();for(const c of connections)if(c.state!=='closed')c.close();await new Promise(resolve=>wss.close(resolve));};
  return {wss,port,connections,received,clientReceived,ends,states,serverStates,createClient,cleanup,restart:()=>{adapter=makeServer();}};
}
test('real WebSocket: round trip, loss, resume and close',{timeout:5000},async()=>{
  const f=await wsFixture();const c=f.createClient();
  try {
    c.connect();await until(()=>c.state==='open'&&f.connections.length===1,'open');
    c.send('one');await until(()=>f.received.length===1,'server receive');f.connections[0].send('reply');
    await until(()=>f.clientReceived.length===1,'client receive');
    [...f.wss.clients][0].terminate();await until(()=>c.state==='disconnected','loss');
    assert.equal(c.send('held').result,'accepted');
    await until(()=>c.state==='open'&&f.received.includes('held'),'resume delivery');
    assert.deepEqual(f.received,['one','held']);assert.deepEqual(f.clientReceived,['reply']);
    c.close();await until(()=>f.ends.length===1,'end');assert.equal(f.ends[0].reason,'closed_by_caller');
  } finally {c.close();await f.cleanup();}
});
test('real WebSocket: restarted server rejects resume with pending count',{timeout:5000},async()=>{
  const f=await wsFixture();const c=f.createClient();let restarted;
  try {
    c.connect();await until(()=>c.state==='open','open');
    [...f.wss.clients][0].terminate();
    await until(()=>c.state==='disconnected','loss');assert.equal(c.send('pending').result,'accepted');
    await new Promise(resolve=>f.wss.close(resolve));
    restarted=new WebSocketServer({port:f.port,host:'127.0.0.1'});
    const fresh=new Server({pingAfter:200,lossAfter:1500,tickInterval:10,expiryAfter:3500});
    restarted.on('connection',socket=>fresh.accept(socket));
    await new Promise(resolve=>restarted.once('listening',resolve));
    await until(()=>f.ends.length===1,'rejected');assert.deepEqual(f.ends[0],{reason:'resume_rejected',unacked:1});
  } finally {c.close();if(restarted){for(const socket of restarted.clients)socket.terminate();await new Promise(resolve=>restarted.close(resolve));}await f.cleanup();}
});
test('real WebSocket: silent TCP proxy triggers loss and resume',{timeout:5000},async()=>{
  const f=await wsFixture(),pipes=[];
  const proxy=createServer(inbound=>{
    const outbound=tcpConnect(f.port,'127.0.0.1');pipes.push({inbound,outbound});
    inbound.on('error',()=>{});outbound.on('error',()=>{});
    inbound.pipe(outbound);outbound.pipe(inbound);
  });
  const port=await listen(proxy),c=f.createClient(`ws://127.0.0.1:${port}`);
  try {
    c.connect();await until(()=>c.state==='open'&&pipes.length===1,'open');
    const {inbound,outbound}=pipes[0];inbound.unpipe(outbound);outbound.unpipe(inbound);inbound.resume();outbound.resume();
    await until(()=>f.states.includes('disconnected'),'silent loss');
    await until(()=>f.serverStates.includes('disconnected'),'server silent loss');
    await until(()=>c.state==='open'&&pipes.length>=2,'silent resume');
    c.send('after silence');await until(()=>f.received.includes('after silence'),'delivery');c.close();
  } finally {
    c.close();for(const p of pipes){p.inbound.destroy();p.outbound.destroy();}
    await new Promise(resolve=>proxy.close(resolve));await f.cleanup();
  }
});
