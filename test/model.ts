export type Phase = 'preparing' | 'open' | 'disconnected' | 'closed';
export type Role = 'client' | 'server';
export type Message = { seq: number; payload: string; bytes: number };
export type RecordState = { id: string; pending: Message[]; sent: number; acked: number; received: number };
export type State = { phase: Phase; role: Role; record: RecordState | null; now: number; expiry: number; expiryDuration: number; attemptUntil: number | null; socket: number | null; ackDue: number | null; pongDue: number | null; lastReceived: number; pingSent: boolean; pingAfter: number; lossAfter: number; maxCount: number; maxBytes: number };
export type Event =
 | { type: 'ready'; socket: number }
 | { type: 'send'; payload: string }
 | { type: 'ack'; seq: number; socket: number }
 | { type: 'data'; seq: number; payload: string; socket: number }
 | { type: 'ping'; socket: number }
 | { type: 'pong'; socket: number }
 | { type: 'loss'; socket: number }
 | { type: 'attempt'; socket: number }
 | { type: 'resume'; seq: number; socket: number }
 | { type: 'reject'; socket: number }
 | { type: 'expiry' }
 | { type: 'close' }
 | { type: 'peerClose'; socket: number }
 | { type: 'replace' }
 | { type: 'noIdentifier' }
 | { type: 'tick'; now: number };
export type Effect =
 | { type: 'result'; result: 'accepted' | 'limit' | 'not_open' | 'closed' | 'ok'; seq?: number }
 | { type: 'frame'; kind: 'data' | 'ack' | 'close' | 'ping' | 'pong'; socket: number; seq?: number; payload?: string; at?: number }
 | { type: 'socket_close'; socket: number }
 | { type: 'deliver'; seq: number; payload: string }
 | { type: 'end'; reason: 'connect_failed' | 'closed_by_caller' | 'peer_closed' | 'protocol_error' | 'resume_rejected' | 'expired' | 'replaced'; count?: number }
 | { type: 'connected' } | { type: 'retry' } | { type: 'new_connection' } | { type: 'resume_reject'; socket: number; reason: 'unknown_identifier' | 'invalid_sequence' } | { type: 'transfer'; record: RecordState };
type Result = { state: State; effects: Effect[] };
type Handler<K extends Event['type']> = (s: State, e: Extract<Event, {type: K}>) => Result;
type Row = { [K in Event['type']]: { description: string; run: Handler<K> } };
const idle = (s: State): Result => ({state:s,effects:[]});
const out = (s: State, ...effects: Effect[]): Result => ({state:s,effects});
const r = (s: State): RecordState => { if (!s.record) throw new Error('missing identity record'); return s.record; };
const finish = (s: State, reason: Extract<Effect,{type:'end'}>['reason']): Result => {
 const rec=s.record; const end: Effect = reason === 'replaced' ? {type:'end',reason} : {type:'end',reason,count:rec?.pending.length ?? 0};
 return out({...s,phase:'closed',record:null,socket:null,attemptUntil:null,ackDue:null,pongDue:null}, ...(reason==='replaced' && rec ? [{type:'transfer',record:rec} as Effect] : []),end);
};
const terminate = (s: State): Result => { const result=finish(s,'closed_by_caller'); return out(result.state,...(s.socket===null?[]:[{type:'frame',kind:'close',socket:s.socket} as Effect]),...result.effects,{type:'result',result:'ok'}); };
const socketMatches = (s: State, socket: number) => s.socket !== null && s.socket === socket;
const send: Handler<'send'> = (s,e) => {
 if (s.phase==='disconnected' && s.now>=s.expiry) return out(s,{type:'result',result:'not_open'});
 const rec=r(s); const seq=rec.sent+1; if(!Number.isSafeInteger(seq)) return out(s,{type:'result',result:'limit'}); const bytes=new TextEncoder().encode(JSON.stringify({type:'data',seq,payload:e.payload})).length;
 if(rec.pending.length>=s.maxCount || rec.pending.reduce((n,m)=>n+m.bytes,0)+bytes>s.maxBytes) return out(s,{type:'result',result:'limit'});
 const message={seq,payload:e.payload,bytes}; const record={...rec,sent:seq,pending:[...rec.pending,message]};
 return out({...s,record},{type:'result',result:'accepted',seq},...(s.phase==='open' && s.socket!==null?[{type:'frame',kind:'data',socket:s.socket,seq,payload:e.payload} as Effect]:[]));
};
const ack: Handler<'ack'> = (s,e) => {
 if(!socketMatches(s,e.socket)) return idle(s); const rec=r(s);
 if(e.seq>rec.sent || e.seq<0 || !Number.isSafeInteger(e.seq)) return finish(s,'protocol_error');
 const acked=Math.max(rec.acked,e.seq); return out({...s,record:{...rec,acked,pending:rec.pending.filter(m=>m.seq>acked)}});
};
const data: Handler<'data'> = (s,e) => {
 if(!socketMatches(s,e.socket)) return idle(s); const rec=r(s);
 if(!Number.isSafeInteger(e.seq) || e.seq<1) return finish(s,'protocol_error');
 if(e.seq<=rec.received) return out({...s,ackDue:s.ackDue ?? s.now+1000});
 if(e.seq!==rec.received+1) return finish(s,'protocol_error');
 return out({...s,record:{...rec,received:e.seq},ackDue:s.ackDue ?? s.now+1000},{type:'deliver',seq:e.seq,payload:e.payload});
};
const loss: Handler<'loss'> = (s,e) => !socketMatches(s,e.socket)?idle(s):out({...s,phase:'disconnected',socket:null,expiry:s.now+s.expiryDuration,attemptUntil:null,ackDue:null,pongDue:null},...(s.role==='client'?[{type:'retry'} as Effect]:[]));
const attempt: Handler<'attempt'> = (s,e) => s.role!=='client'||s.attemptUntil!==null||s.now>=s.expiry ? idle(s):out({...s,socket:e.socket,attemptUntil:s.now+10000});
const disconnectedLoss: Handler<'loss'> = (s,e) => { if(!socketMatches(s,e.socket)) return idle(s); const n={...s,socket:null,attemptUntil:null}; return s.now>=s.expiry?finish(n,'expired'):out(n,...(s.role==='client'?[{type:'retry'} as Effect]:[])); };
const resume: Handler<'resume'> = (s,e) => {
 if(s.role==='client' && (!socketMatches(s,e.socket)||s.attemptUntil===null)) return idle(s);
 if(s.role==='client' && s.now>=s.attemptUntil!) return tick(s,{type:'tick',now:s.now});
 if(s.role==='server' && s.now>=s.expiry) { const f=finish(s,'expired'); return out(f.state,...f.effects,{type:'resume_reject',socket:e.socket,reason:'unknown_identifier'}); }
 const rec=r(s); if(e.seq>rec.sent || e.seq<0 || !Number.isSafeInteger(e.seq)) return s.role==='server'?out(s,{type:'resume_reject',socket:e.socket,reason:'invalid_sequence'}):finish(s,'protocol_error');
 const acked=Math.max(rec.acked,e.seq); const record={...rec,acked,pending:rec.pending.filter(m=>m.seq>acked)};
 return out({...s,phase:'open',socket:e.socket,attemptUntil:null,ackDue:null,pongDue:null,lastReceived:s.now,pingSent:false,record},{type:'connected'},...record.pending.map(m=>({type:'frame',kind:'data',socket:e.socket,seq:m.seq,payload:m.payload} as Effect)));
};
const tick: Handler<'tick'> = (s,e) => {
 if(e.now<s.now) throw new Error('clock moved backwards'); const t={...s,now:e.now};
 if(t.phase==='disconnected') {
  if(t.attemptUntil!==null && t.now>=t.attemptUntil) { const n={...t,socket:null,attemptUntil:null}; const result=t.now>=t.expiry?finish(n,'expired'):out(n,{type:'retry'}); return out(result.state,...(t.socket===null?[]:[{type:'socket_close',socket:t.socket} as Effect]),...result.effects); }
  if(t.now>=t.expiry && t.attemptUntil===null) return finish(t,'expired');
 }
 if(t.phase==='preparing' && t.now-t.lastReceived>=t.lossAfter) return finish(t,'connect_failed');
 if(t.phase==='open' && t.socket!==null) {
  if(t.now-t.lastReceived>=t.lossAfter) { const lost=loss(t,{type:'loss',socket:t.socket}); return out(lost.state,{type:'socket_close',socket:t.socket},...lost.effects); }
  const effects: Effect[]=[]; let n=t;
  if(t.ackDue!==null && t.now>=t.ackDue) { effects.push({type:'frame',kind:'ack',socket:t.socket,seq:r(t).received,at:t.ackDue}); n={...n,ackDue:null}; }
  if(t.pongDue!==null && t.now>=t.pongDue) { effects.push({type:'frame',kind:'pong',socket:t.socket,at:t.pongDue}); n={...n,pongDue:null}; }
  if(!t.pingSent && t.now-t.lastReceived>=t.pingAfter) { effects.push({type:'frame',kind:'ping',socket:t.socket,at:t.lastReceived+t.pingAfter}); n={...n,pingSent:true}; }
  return out(n,...effects);
 }
 return out(t);
};
const ignore = {description:'無視',run:idle};
const matrix = {
 preparing: {
  ready:{description:'開へ、接続完了通知',run:(s,e)=>out({...s,phase:'open',socket:e.socket,lastReceived:s.now,pingSent:false},{type:'connected'})},
  send:{description:'not_open',run:s=>out(s,{type:'result',result:'not_open'})},
  ack:{description:'現 socket は protocol_error、旧 socket は無視',run:(s,e)=>socketMatches(s,e.socket)?finish(s,'protocol_error'):idle(s)},
  data:ignore,ping:ignore,pong:ignore, loss:{description:'connect_failed、識別子破棄',run:(s,e)=>socketMatches(s,e.socket)?finish(s,'connect_failed'):idle(s)}, attempt:ignore,resume:ignore,reject:ignore,expiry:ignore,
  close:{description:'closed_by_caller、件数 0',run:terminate}, peerClose:{description:'peer_closed、件数 0',run:(s,e)=>socketMatches(s,e.socket)?finish(s,'peer_closed'):idle(s)},replace:ignore,
  noIdentifier:{description:'server は新規接続',run:s=>s.role==='server'?out(s,{type:'new_connection'}):idle(s)},tick:{description:'lossAfter（既定 30 秒）で connect_failed',run:tick}
 },
 open: {
  ready:ignore,send:{description:'枠内なら受理し送出、超過は limit',run:send},ack:{description:'累積 ack を解放、最大超過は protocol_error',run:ack},data:{description:'順に配送し ack を予約、重複は破棄',run:data},ping:{description:'現 socket に pong を予約',run:(s,e)=>socketMatches(s,e.socket)?out({...s,pongDue:s.pongDue ?? s.now+1000}):idle(s)},pong:{description:'受信を記録',run:idle},loss:{description:'切断中へ、期限開始',run:loss},attempt:ignore,
  resume:{description:'server は無効なら拒否し旧接続維持、有効なら記録を引き継ぎ replaced',run:(s,e)=>{ if(s.role!=='server') return idle(s); const rec=r(s); if(e.seq>rec.sent || e.seq<0 || !Number.isSafeInteger(e.seq)) return out(s,{type:'resume_reject',socket:e.socket,reason:'invalid_sequence'}); const acked=Math.max(rec.acked,e.seq); return finish({...s,record:{...rec,acked,pending:rec.pending.filter(m=>m.seq>acked)}},'replaced'); }},reject:ignore,expiry:ignore,
  close:{description:'close frame と closed_by_caller',run:terminate},peerClose:{description:'peer_closed',run:(s,e)=>socketMatches(s,e.socket)?finish(s,'peer_closed'):idle(s)},replace:{description:'記録を引き継ぎ replaced 通知',run:s=>finish(s,'replaced')},
  noIdentifier:{description:'server は旧接続を保ち新規接続',run:s=>s.role==='server'?out(s,{type:'new_connection'}):idle(s)},tick:{description:'ack・pong・ping・無通信期限を判定',run:tick}
 },
 disconnected: {
  ready:ignore,send:{description:'期限前は枠内で受理、期限後 not_open',run:send},ack:ignore,data:ignore,ping:ignore,pong:ignore,loss:{description:'試行だけ終了し再試行、期限後は expired',run:disconnectedLoss},attempt:{description:'client の再開試行開始、10 秒待つ',run:attempt},
  resume:{description:'有効なら開き累積 ack と再送、期限境界を判定',run:resume},reject:{description:'client は resume_rejected',run:(s,e)=>s.role==='client' && socketMatches(s,e.socket)?finish(s,'resume_rejected'):idle(s)},
  expiry:{description:'待機中は応答または 10 秒まで保持',run:s=>tick(s,{type:'tick',now:Math.max(s.now,s.expiry)})},close:{description:'closed_by_caller',run:terminate},peerClose:{description:'現在の socket なら peer_closed',run:(s,e)=>socketMatches(s,e.socket)?finish(s,'peer_closed'):idle(s)},replace:ignore,
  noIdentifier:{description:'server は別の新規接続、旧記録を維持',run:s=>s.role==='server'?out(s,{type:'new_connection'}):idle(s)},tick:{description:'10 秒で試行 socket を閉じて再試行または満了、待機中は期限で満了',run:tick}
 },
 closed: {
  ready:ignore,send:{description:'closed',run:s=>out(s,{type:'result',result:'closed'})},ack:ignore,data:ignore,ping:ignore,pong:ignore,loss:ignore,attempt:ignore,resume:{description:'server は未知の識別子として拒否',run:(s,e)=>s.role==='server'?out(s,{type:'resume_reject',socket:e.socket,reason:'unknown_identifier'}):idle(s)},reject:ignore,expiry:ignore,
  close:{description:'成功、通知なし',run:s=>out(s,{type:'result',result:'ok'})},peerClose:ignore,replace:ignore,noIdentifier:{description:'server は新規接続',run:s=>s.role==='server'?out(s,{type:'new_connection'}):idle(s)},tick:{description:'時計のみ進む',run:tick}
 }
} satisfies Record<Phase,Row>;
export const descriptions: Record<Phase, Record<Event['type'],string>> = Object.fromEntries(Object.entries(matrix).map(([p,row])=>[p,Object.fromEntries(Object.entries(row).map(([e,v])=>[e,v.description]))])) as Record<Phase, Record<Event['type'],string>>;
export function step(state: State, event: Event): Result {
 if(state.phase==='open' && 'socket' in event && socketMatches(state,event.socket) && ['ack','data','ping','pong','peerClose','resume','reject'].includes(event.type)) state={...state,lastReceived:state.now,pingSent:false};
 const handler=matrix[state.phase][event.type].run as (s:State,e:Event)=>Result;
 return handler(state,event);
}
export function initial(role: Role, id='id', socket=1): State { return {phase:'preparing',role,record:{id,pending:[],sent:0,acked:0,received:0},now:0,expiry:Infinity,expiryDuration:600000,attemptUntil:null,socket,ackDue:null,pongDue:null,lastReceived:0,pingSent:false,pingAfter:15000,lossAfter:30000,maxCount:1000,maxBytes:8*1024*1024}; }
export function successor(s: State, record: RecordState, socket: number): Result { const state={...s,phase:'open' as const,record,socket,attemptUntil:null,ackDue:null,pongDue:null,lastReceived:s.now,pingSent:false}; return out(state,{type:'connected'},...record.pending.map(m=>({type:'frame',kind:'data',socket,seq:m.seq,payload:m.payload} as Effect))); }
