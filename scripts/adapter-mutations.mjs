import {readFileSync,writeFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
const mutations=[
  ['client.ts','no hello','socket.send(JSON.stringify(hello));','void 0;'],
  ['transport.ts','no outgoing core frame','socket.send(JSON.stringify(effect.frame));','void 0;'],
  ['transport.ts','no receive callback',"this.callbacks.onReceive?.(effect.data,effect.seq);","void 0;"],
  ['client.ts','no retry dial',"if(this.state==='disconnected')this.dial();","void 0;"],
  ['client.ts','ignore resume rejection',"this.dispatch({type:'reject',socket:id});","void 0;"],
  ['transport.ts','no liveness deadline',"if(this.state==='open'||this.state==='preparing')this.advance();","void 0;"],
  ['server.ts','no initial welcome',"send({type:'welcome',identifier,seq:0,resumed:false});","void 0;"],
  ['server.ts','no resume welcome',"send({type:'welcome',identifier:prior.identifier,seq:received,resumed:true});","void 0;"],
  ['server.ts','opens before ready',"this.options.onConnection?.(session);\n        }else {","this.options.onConnection?.(session);session.initial(id);\n        }else {"],
  ['server.ts','resume effect before reply',"send({type:'welcome',identifier:prior.identifier,seq:received,resumed:true});\n          const result=prior.resume(id,seq);","const result=prior.resume(id,seq);prior.apply(result);\n          send({type:'welcome',identifier:prior.identifier,seq:received,resumed:true});"],
  ['server.ts','preparing frame extends deadline',"this.advance();\n    if(frame.type==='ready'","this.advance();\n    if(this.state==='preparing')this.core.beginPreparing(this.clock.now());\n    if(frame.type==='ready'"],
  ['client.ts','retry without jitter',"const delay=base+(Math.min(maximum,base*1.1)-base)*Math.max(0,Math.min(1,random()));","const delay=base;"],
  ['client.ts','retry exceeds ten seconds',"const maximum=Math.min(10000,this.options.retryMax??10000);","const maximum=this.options.retryMax??10000;"]
];
let failed=0;
for(const [name,label,from,to] of mutations){
  const file=new URL(`../src/${name}`,import.meta.url),original=readFileSync(file,'utf8');
  try {
    if(original.split(from).length!==2){console.log(`INVALID ${label}: target not unique`);failed++;continue;}
    writeFileSync(file,original.replace(from,to));
    const build=spawnSync('npm',['run','build'],{encoding:'utf8'});
    if(build.status!==0){console.log(`INVALID ${label}: compilation failed`);failed++;continue;}
    const run=spawnSync('node',['--test','test/adapter.test.mjs'],{encoding:'utf8',timeout:10000});
    const killed=run.status!==0&&run.signal===null;
    console.log(`${killed?'KILLED':'SURVIVED'} ${label}`);if(!killed)failed++;
  } finally {writeFileSync(file,original);}
}
const restore=spawnSync('npm',['run','build'],{encoding:'utf8'});
if(restore.status!==0){console.log('RESTORE BUILD FAILED');failed++;}
console.log(`adapter mutations=${mutations.length} failed=${failed}`);
if(failed)process.exitCode=1;
