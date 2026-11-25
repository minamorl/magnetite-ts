export type Role = 'client' | 'server';
export type State = 'preparing' | 'open' | 'disconnected' | 'closed';
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Frame = { type: 'data'; seq: number; data: Json } | { type: 'ack'; seq: number } | { type: 'ping' | 'pong' | 'close' };
export type EndReason = 'connect_failed' | 'protocol_error' | 'expired' | 'resume_rejected' | 'closed_by_caller' | 'peer_closed' | 'replaced';
export type Effect = { type: 'frame'; socket: string; frame: Frame; at: number } | { type: 'closeSocket'; socket: string } | { type: 'connected' | 'retry' } | { type: 'deliver'; seq: number; data: Json } | { type: 'ended'; reason: EndReason; unacked?: number } | { type: 'reject'; socket: string; reason: 'invalid_sequence' | 'unknown_identifier' } | { type: 'newConnection'; socket: string } | { type: 'transfer'; socket?: string; record: RecordData };
export type RecordData = { identifier: string; sent: number; received: number; acknowledged: number; pending: {seq: number; data: Json; bytes: number}[] };
export type Event = {type:'ready'; socket:string} | {type:'send'; data:Json} | {type:'ack' | 'data' | 'ping' | 'pong' | 'loss' | 'peerClose' | 'reject'; socket:string; seq?:number; data?:Json} | {type:'attempt'; socket:string} | {type:'resume'; socket:string; seq:number} | {type:'noIdentifier'; socket:string} | {type:'replace'} | {type:'expiry'; now:number} | {type:'tick'; now:number} | {type:'close'};
export interface Options {role:Role; identifier:string; now:number; socket?:string; expiryAfter?:number; pingAfter?:number; lossAfter?:number; maxCount?:number; maxBytes?:number; record?:RecordData; successor?:boolean}
export type Result = {effects:Effect[]; result?:'accepted' | 'limit' | 'not_open' | 'closed' | 'ok' | 'error'; seq?:number};
/** The caller supplies cryptographically unpredictable bytes and UTC milliseconds. */
export function issueIdentifier(now:number, randomBytes:(length:number)=>Uint8Array):string {
  if(!Number.isSafeInteger(now)||now<0||now>281474976710655)throw new RangeError('invalid timestamp');
  const bytes=randomBytes(16);
  if(bytes.length!==16)throw new RangeError('invalid random length');
  let time=BigInt(now);
  for(let i=5;i>=0;i--){bytes[i]=Number(time&255n);time>>=8n;}
  bytes[6]=(bytes[6]&15)|112;bytes[8]=(bytes[8]&63)|128;
  const hex=Array.from(bytes,b=>b.toString(16).padStart(2,'0')).join('');
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
}
const copy = (r:RecordData):RecordData => ({...r,pending:r.pending.map(p=>({...p}))});
export class Core {
  readonly role:Role; state:State; now:number; socket?:string; record:RecordData;
  readonly expiryAfter:number; readonly pingAfter:number; readonly lossAfter:number; readonly maxCount:number; readonly maxBytes:number;
  private since:number; private deadline=0; private attemptUntil?:number; private ackAt?:number; private pongAt?:number; private pingSent=false; private retired=new Set<string>();
  constructor(o:Options) {
    this.role=o.role; this.now=o.now; this.since=o.now; this.socket=o.socket;
    this.expiryAfter=o.expiryAfter??600000; this.pingAfter=o.pingAfter??15000; this.lossAfter=o.lossAfter??30000; this.maxCount=o.maxCount??1000; this.maxBytes=o.maxBytes??8388608;
    this.record=o.record?copy(o.record):{identifier:o.identifier,sent:0,received:0,acknowledged:0,pending:[]};
    this.state=o.successor?'open':'preparing';
  }
  beginPreparing(now:number):void {if(this.state==='preparing'){if(now<this.now)throw new RangeError('clock moved backwards');this.now=now;this.since=now;}}
  successorEffects():Effect[] { if(this.state!=='open'||!this.socket) return []; return [{type:'connected'},...this.record.pending.map(p=>this.frame(this.socket!,{type:'data',seq:p.seq,data:p.data},this.now))]; }
  private frame(socket:string,frame:Frame,at:number):Effect {return {type:'frame',socket,frame,at};}
  private terminal(reason:EndReason,effects:Effect[],replaced=false) { if(this.state==='closed') return; const n=this.record.pending.length; this.state='closed'; if(this.socket!==undefined)this.retired.add(this.socket); this.socket=undefined; this.attemptUntil=undefined; this.ackAt=undefined; this.pongAt=undefined; this.record={identifier:'',sent:0,received:0,acknowledged:0,pending:[]}; effects.push(replaced?{type:'ended',reason}:{type:'ended',reason,unacked:n}); }
  private acknowledge(seq:number) { if(seq>this.record.acknowledged){this.record.acknowledged=seq;this.record.pending=this.record.pending.filter(p=>p.seq>seq);} }
  private invalid(seq:number) {return !Number.isSafeInteger(seq)||seq<0||seq>this.record.sent;}
  private open(socket:string,e:Effect[]) {this.socket=socket;this.state='open';this.since=this.now;this.pingSent=false;this.attemptUntil=undefined;e.push({type:'connected'});for(const p of this.record.pending)e.push(this.frame(socket,{type:'data',seq:p.seq,data:p.data},this.now));}
  private lost(e:Effect[]) {this.state='disconnected';this.deadline=this.now+this.expiryAfter;if(this.socket!==undefined)this.retired.add(this.socket);this.socket=undefined;this.ackAt=undefined;this.pongAt=undefined;this.attemptUntil=undefined;if(this.role==='client')e.push({type:'retry'});}
  private endAttempt(e:Effect[]) {this.attemptUntil=undefined;if(this.socket!==undefined)this.retired.add(this.socket);this.socket=undefined;if(this.now>=this.deadline)this.terminal('expired',e);else if(this.role==='client')e.push({type:'retry'});}
  private timeoutAttempt(e:Effect[]) {const socket=this.socket;if(socket!==undefined){this.retired.add(socket);this.socket=undefined;e.push({type:'closeSocket',socket});}this.endAttempt(e);}
  dispatch(event:Event):Result {
    const effects:Effect[]=[]; const out:Result={effects};
    if('socket' in event&&this.retired.has(event.socket))return out;
    if(event.type==='tick') {if(event.now<this.now){out.result='error';return out;} this.now=event.now;}
    if(event.type==='expiry'&&this.state==='disconnected')this.now=Math.max(this.now,this.deadline,event.now);
    if(this.state==='open' && 'socket' in event && event.socket===this.socket && ['ready','ack','data','ping','pong','resume','reject','peerClose'].includes(event.type)){this.since=this.now;this.pingSent=false;}
    if(event.type==='send') {
      if(this.state==='closed'){out.result='closed';return out;}
      if(this.state==='preparing'||(this.state==='disconnected'&&this.now>=this.deadline)){out.result='not_open';return out;}
      const seq=this.record.sent+1; const frame:Frame={type:'data',seq,data:event.data}; const bytes=new TextEncoder().encode(JSON.stringify(frame)).length;
      if(!Number.isSafeInteger(seq)||this.record.pending.length>=this.maxCount||this.record.pending.reduce((n,p)=>n+p.bytes,0)+bytes>this.maxBytes){out.result='limit';return out;}
      this.record.sent=seq;this.record.pending.push({seq,data:event.data,bytes});out.result='accepted';out.seq=seq;if(this.state==='open'&&this.socket)effects.push(this.frame(this.socket,frame,this.now));return out;
    }
    if(event.type==='tick') {
      if(this.state==='preparing'&&this.now-this.since>=this.lossAfter)this.terminal('connect_failed',effects);
      else if(this.state==='open'&&this.socket){
        if(this.now-this.since>=this.lossAfter){effects.push({type:'closeSocket',socket:this.socket});this.lost(effects);}
        else {if(this.ackAt!==undefined&&this.now>=this.ackAt){effects.push(this.frame(this.socket,{type:'ack',seq:this.record.received},this.ackAt));this.ackAt=undefined;}if(this.pongAt!==undefined&&this.now>=this.pongAt){effects.push(this.frame(this.socket,{type:'pong'},this.pongAt));this.pongAt=undefined;}if(!this.pingSent&&this.now-this.since>=this.pingAfter){effects.push(this.frame(this.socket,{type:'ping'},this.since+this.pingAfter));this.pingSent=true;}}
      } else if(this.state==='disconnected'){if(this.attemptUntil!==undefined&&this.now>=this.attemptUntil){this.timeoutAttempt(effects);}else if(this.attemptUntil===undefined&&this.now>=this.deadline)this.terminal('expired',effects);}
      return out;
    }
    if(event.type==='expiry'){if(this.state==='disconnected'&&this.now>=this.deadline){if(this.attemptUntil!==undefined&&this.now>=this.attemptUntil)this.timeoutAttempt(effects);else if(this.attemptUntil===undefined)this.terminal('expired',effects);}return out;}
    if(event.type==='close'){if(this.state!=='closed'){if(this.socket)effects.push(this.frame(this.socket,{type:'close'},this.now));this.terminal('closed_by_caller',effects);}out.result='ok';return out;}
    if(event.type==='noIdentifier'){if(this.role==='server')effects.push({type:'newConnection',socket:event.socket});return out;}
    if(event.type==='resume'&&this.state==='closed'){if(this.role==='server')effects.push({type:'reject',socket:event.socket,reason:'unknown_identifier'});return out;}
    if(this.state==='closed')return out;
    if(event.type==='ready'){if(this.state==='preparing'){this.state='open';this.socket=event.socket;this.since=this.now;this.pingSent=false;effects.push({type:'connected'});}return out;}
    if(event.type==='attempt'){if(this.state==='disconnected'&&this.role==='client'&&this.attemptUntil===undefined&&this.now<this.deadline){this.socket=event.socket;this.attemptUntil=this.now+10000;}return out;}
    if(event.type==='resume') {
      if(this.state==='open'&&this.role==='server'){if(this.invalid(event.seq))effects.push({type:'reject',socket:event.socket,reason:'invalid_sequence'});else {this.acknowledge(event.seq);effects.push({type:'transfer',socket:event.socket,record:copy(this.record)});this.terminal('replaced',effects,true);}return out;}
      if(this.state!=='disconnected')return out;
      if(this.role==='client'&&(this.attemptUntil===undefined||event.socket!==this.socket))return out;
      if(this.role==='client'&&this.attemptUntil!==undefined&&this.now>=this.attemptUntil){this.timeoutAttempt(effects);return out;}
      if(this.role==='server'&&this.now>=this.deadline){this.terminal('expired',effects);effects.push({type:'reject',socket:event.socket,reason:'unknown_identifier'});return out;}
      if(this.invalid(event.seq)){if(this.role==='server')effects.push({type:'reject',socket:event.socket,reason:'invalid_sequence'});else this.terminal('protocol_error',effects);return out;}
      this.acknowledge(event.seq);this.open(event.socket,effects);return out;
    }
    if(event.type==='replace'){if(this.state==='open'){effects.push({type:'transfer',record:copy(this.record)});this.terminal('replaced',effects,true);}return out;}
    if(event.type==='reject'){if(this.state==='disconnected'&&this.role==='client'&&event.socket===this.socket)this.terminal('resume_rejected',effects);return out;}
    if(event.type==='loss'){if(event.socket!==this.socket)return out;if(this.state==='preparing')this.terminal('connect_failed',effects);else if(this.state==='open')this.lost(effects);else if(this.state==='disconnected')this.endAttempt(effects);return out;}
    if(event.type==='peerClose'){if(event.socket===this.socket)this.terminal('peer_closed',effects);return out;}
    if(event.type==='ack'){if(event.socket!==this.socket)return out;if(this.state==='preparing'){this.terminal('protocol_error',effects);return out;}if(this.state==='open'){const n=event.seq??NaN;if(this.invalid(n))this.terminal('protocol_error',effects);else this.acknowledge(n);}return out;}
    if(this.state!=='open'||event.socket!==this.socket)return out;
    if(event.type==='data'){const n=event.seq??NaN;if(!Number.isSafeInteger(n)||n<1||n>this.record.received+1){this.terminal('protocol_error',effects);return out;}if(n===this.record.received+1){this.record.received=n;effects.push({type:'deliver',seq:n,data:event.data??null});}this.ackAt??=this.now+1000;}
    if(event.type==='ping')this.pongAt??=this.now+1000;
    return out;
  }
}
