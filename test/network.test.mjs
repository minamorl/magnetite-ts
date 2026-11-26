import {test} from 'node:test';
import assert from 'node:assert/strict';
import {Core} from '../dist/core.js';

// The caller owns the resume handshake. Only queued control packets or frame
// effects emitted by a Core are delivered; queued packets on a broken link die.
function scenario(random, index) {
  let now=0, generation=0, link='s0';
  let a=new Core({role:'client',identifier:'id',now,socket:link,maxCount:2,expiryAfter:25000});
  let b=new Core({role:'server',identifier:'id',now,socket:link,maxCount:2,expiryAfter:25000});
  a.dispatch({type:'ready',socket:link}); b.dispatch({type:'ready',socket:link});
  const delivered=[new Map(),new Map()], accepted=[new Map(),new Map()], discarded=[new Set(),new Set()];
  const queue=[];
  function add(side,result,before=[]) {
    for (const e of result.effects) {
      if(e.type==='frame') queue.push({side,socket:e.socket,frame:e.frame});
      if(e.type==='deliver') {
        const map=delivered[side]; map.set(e.seq,(map.get(e.seq)??0)+1);
        assert.equal(map.get(e.seq),1,`duplicate ${index}:${side}:${e.seq}`);
      }
      if(e.type==='ended'&&e.reason!=='replaced') {
        assert.equal(e.unacked,before.length);
        for(const seq of before)discarded[side].add(seq);
      }
    }
  }
  function send(side,value) {
    const c=side===0?a:b, r=c.dispatch({type:'send',data:value});add(side,r);
    if(r.result==='accepted')accepted[side].set(r.seq,value);
  }
  function drain() {
    while(queue.length) {
      const item=queue.shift();
      if(item.socket!==link)continue;
      const dst=item.side===0?b:a;
      if(dst.state!=='open'||dst.socket!==link)continue;
      const f=item.frame;
      if(f.type==='close'){const before=dst.record.pending.map(p=>p.seq);add(1-item.side,dst.dispatch({type:'peerClose',socket:link}),before);}
      else add(1-item.side,dst.dispatch({...f,socket:link}));
    }
  }
  function tick(ms) {now+=ms;for(const [side,c] of [[0,a],[1,b]]){const before=c.record.pending.map(p=>p.seq);add(side,c.dispatch({type:'tick',now}),before);}}
  function cut() {
    queue.length=0;
    const mode=random();
    if(mode<0.08){tick(30000);return;}
    if(mode<0.16){
      const c=random()<0.5?a:b, side=c===a?0:1;
      if(c.state==='open')add(side,c.dispatch({type:'loss',socket:link}));
      tick(30000);return;
    }
    if(a.state==='open')add(0,a.dispatch({type:'loss',socket:link}));
    if(b.state==='open')add(1,b.dispatch({type:'loss',socket:link}));
  }
  for(let n=0;n<3;n++) {
    send(0,`a${n}`);send(1,`b${n}`);
    if(random()<0.5){drain();tick(1000);if(random()<0.5)drain();}
    cut();
    if(a.state==='closed'||b.state==='closed')break;
    let resumed=false;
    for(let attempt=0;attempt<3&&!resumed;attempt++) {
      link=`s${++generation}`;
      a.dispatch({type:'attempt',socket:link});
      // A resume request and reply are emitted by this caller and may be lost.
      const controls=[];
      controls.push({type:'resume-request',socket:link,seq:a.record.received});
      if(random()<0.16){controls.length=0;tick(10000);queue.length=0;continue;}
      const request=controls.shift();
      const serverReplySeq=b.record.received;
      const response=b.dispatch({type:'resume',socket:request.socket,seq:request.seq});
      const transfer=response.effects.find(e=>e.type==='transfer');
      if(transfer) {
        b=new Core({role:'server',identifier:transfer.record.identifier,record:transfer.record,now,socket:link,successor:true,maxCount:2,expiryAfter:25000});
        add(1,{effects:b.successorEffects()});
      } else add(1,response);
      if(b.state==='closed')break;
      controls.push({type:'resume-reply',socket:link,seq:serverReplySeq});
      if(random()<0.16){controls.length=0;tick(10000);queue.length=0;continue;}
      const reply=controls.shift();
      add(0,a.dispatch({type:'resume',socket:reply.socket,seq:reply.seq}));
      if(a.state==='open'){resumed=true;drain();tick(1000);drain();}
    }
    if(!resumed)break;
  }
  // A terminal notice counts every remaining unacknowledged send, including
  // messages already delivered whose acknowledgement was lost.
  if(a.state==='open'&&b.state==='open'&&random()<0.5){
    const before=a.record.pending.map(p=>p.seq);
    add(0,a.dispatch({type:'close'}),before);drain();
  }
  for(const [side,c] of [[0,a],[1,b]]) {
    const pending=new Set(c.record.pending.map(p=>p.seq));
    for(const seq of accepted[side].keys()) {
      assert.ok(delivered[1-side].get(seq)===1||pending.has(seq)||discarded[side].has(seq),`lost ${index}:${side}:${seq}`);
    }
    if(c.state!=='closed') {
      const result=c.dispatch({type:'close'});
      assert.equal(result.effects.find(e=>e.type==='ended').unacked,pending.size);
    }
  }
}

for(const seed of [0x12345678,0x9abcdef0,0x31415926]) {
  test(`network seed ${seed.toString(16)}: 10000 scenarios`,()=>{
    let s=seed>>>0;
    const random=()=>{s=(Math.imul(s,1664525)+1013904223)>>>0;return s/4294967296;};
    for(let i=0;i<10000;i++)scenario(random,i);
  });
}
