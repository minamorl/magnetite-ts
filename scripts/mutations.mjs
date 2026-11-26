import {readFileSync,writeFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';

const file=new URL('../src/core.ts',import.meta.url);
const original=readFileSync(file,'utf8');
const mutations=[
  ['no ping',"effects.push(this.frame(this.socket,{type:'ping'},this.since+this.pingAfter));","void 0;"],
  ['no liveness loss',"this.now-this.since>=this.lossAfter){effects.push","this.now-this.since>this.lossAfter){effects.push"],
  ['no pong',"effects.push(this.frame(this.socket,{type:'pong'},this.pongAt));","void 0;"],
  ['no successor retransmit',"...this.record.pending.map(p=>this.frame(this.socket!,{type:'data',seq:p.seq,data:p.data},this.now))","...[]"],
  ['no resume retransmit',"for(const p of this.record.pending)e.push(this.frame(socket,{type:'data',seq:p.seq,data:p.data},this.now));","void 0;"],
  ['no ack scheduling',"this.ackAt??=this.now+1000;","void 0;"],
  ['deliver duplicate',"if(n===this.record.received+1){this.record.received=n;effects.push","if(n<=this.record.received+1){this.record.received=n;effects.push"],
  ['accept sequence gap',"n>this.record.received+1","n>this.record.received+2"],
  ['ack does not release',"this.record.pending=this.record.pending.filter(p=>p.seq>seq);","void 0;"],
  ['no count limit',"this.record.pending.length>=this.maxCount","false"],
  ['attempt timeout late',"else if(this.state==='disconnected'){if(this.attemptUntil!==undefined&&this.now>=this.attemptUntil)","else if(this.state==='disconnected'){if(this.attemptUntil!==undefined&&this.now>this.attemptUntil)"],
  ['wrong terminal count',"const n=this.record.pending.length;","const n=0;"],
  ['no transfer',"effects.push({type:'transfer',record:copy(this.record)});","void 0;"],
  ['lost socket remains current',"if(this.socket!==undefined)this.retired.add(this.socket);this.socket=undefined;this.ackAt=undefined;this.pongAt=undefined;this.attemptUntil=undefined;if(this.role==='client')","this.ackAt=undefined;this.pongAt=undefined;this.attemptUntil=undefined;if(this.role==='client')"],
  ['attempt timeout does not close',"e.push({type:'closeSocket',socket});","void 0;"],
  ['close retains ack and pong deadlines',"this.ackAt=undefined; this.pongAt=undefined; this.record=","this.record="]
];
let failed=0;
try {
  for(const [name,from,to] of mutations){
    if(original.split(from).length!==2){console.log(`INVALID ${name}: target not unique`);failed++;continue;}
    writeFileSync(file,original.replace(from,to));
    const build=spawnSync('npm',['run','build'],{encoding:'utf8'});
    if(build.status!==0){console.log(`INVALID ${name}: compilation failed`);failed++;continue;}
    const run=spawnSync('node',['--test','test/lifecycle.test.mjs','test/network.test.mjs','test/v14.test.mjs'],{encoding:'utf8'});
    const killed=run.status!==0&&run.signal===null;
    console.log(`${killed?'KILLED':'SURVIVED'} ${name}`);
    if(!killed)failed++;
  }
} finally {
  writeFileSync(file,original);
  const restore=spawnSync('npm',['run','build'],{encoding:'utf8'});
  if(restore.status!==0){console.log('RESTORE BUILD FAILED');failed++;}
}
console.log(`mutations=${mutations.length} failed=${failed}`);
if(failed)process.exitCode=1;
