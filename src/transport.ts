import {Core, type Effect, type EndReason, type Event, type Frame, type Json, type Result, type State} from './core.js';

export interface SocketLike {
  readonly readyState:number;
  send(data:string):void;
  close():void;
  addEventListener(type:string, listener:(event:any)=>void):void;
}
export interface Clock {
  now():number;
  setTimeout(callback:()=>void,delay:number):unknown;
  clearTimeout(handle:unknown):void;
}
export const systemClock:Clock={
  now:()=>Date.now(),
  setTimeout:(f,ms)=>globalThis.setTimeout(f,ms),
  clearTimeout:h=>globalThis.clearTimeout(h as ReturnType<typeof setTimeout>)
};
export interface Callbacks {
  onReceive?:(data:Json,seq:number)=>void;
  onState?:(state:State)=>void;
  onEnd?:(notice:{reason:EndReason;unacked?:number})=>void;
}
export interface Timing { clock?:Clock; tickInterval?:number; expiryAfter?:number; pingAfter?:number; lossAfter?:number; maxCount?:number; maxBytes?:number }
export type Wire = Frame | {type:'hello';identifier?:string;seq?:number} | {type:'ready'} | {type:'welcome';identifier:string;seq:number;resumed:boolean} | {type:'reject';reason:string};
export function parseWire(data:unknown):Wire|undefined {
  if(typeof data!=='string')return undefined;
  try {const v:unknown=JSON.parse(data);if(!v||typeof v!=='object'||typeof (v as {type?:unknown}).type!=='string')return undefined;return v as Wire;}catch{return undefined;}
}
export abstract class Endpoint {
  protected core:Core;
  protected readonly clock:Clock;
  protected readonly interval:number;
  protected sockets=new Map<string,SocketLike>();
  protected timer?:unknown;
  private deadlines=new Set<unknown>();
  private liveness=new Set<unknown>();
  private readonly pingAfter:number;
  private readonly lossAfter:number;
  private effects:Effect[]=[];
  private processing=false;
  protected callbacks:Callbacks;
  constructor(core:Core, options:Timing, callbacks:Callbacks) {
    this.core=core;this.clock=options.clock??systemClock;this.pingAfter=options.pingAfter??15000;this.lossAfter=options.lossAfter??30000;this.interval=options.tickInterval??Math.max(1,Math.min(250,this.pingAfter/4,this.lossAfter/4));this.callbacks=callbacks;
  }
  get state():State{return this.core.state;}
  protected abstract retry():void;
  protected process(result:Result):void {
    this.effects.push(...result.effects);
    if(this.processing)return;
    this.processing=true;
    try {while(this.effects.length)this.effect(this.effects.shift()!);}
    finally {this.processing=false;}
  }
  protected effect(effect:Effect):void {
    if(effect.type==='frame'){
      const socket=this.sockets.get(effect.socket);
      if(socket&&socket.readyState===1){try{socket.send(JSON.stringify(effect.frame));}catch{this.process(this.core.dispatch({type:'loss',socket:effect.socket}));}}
    } else if(effect.type==='closeSocket'){
      const socket=this.sockets.get(effect.socket);this.sockets.delete(effect.socket);if(socket)socket.close();
    } else if(effect.type==='connected'){this.scheduleLiveness();this.callbacks.onState?.('open');}
    else if(effect.type==='retry'){this.callbacks.onState?.('disconnected');this.retry();}
    else if(effect.type==='deliver')this.callbacks.onReceive?.(effect.data,effect.seq);
    else if(effect.type==='ended'){this.stopTimer();for(const socket of this.sockets.values())socket.close();this.sockets.clear();this.callbacks.onState?.('closed');this.callbacks.onEnd?.({reason:effect.reason,...(effect.unacked===undefined?{}:{unacked:effect.unacked})});}
  }
  protected advance():void {const before=this.core.state;const r=this.core.dispatch({type:'tick',now:this.clock.now()});this.process(r);if(before==='open'&&this.core.state==='disconnected'&&!r.effects.some(e=>e.type==='retry'))this.callbacks.onState?.('disconnected');}
  protected runTimer():void {
    if(this.timer!==undefined||this.state==='closed')return;
    if(this.state==='preparing'&&this.liveness.size===0)this.scheduleLiveness();
    this.timer=this.clock.setTimeout(()=>{this.timer=undefined;this.advance();this.runTimer();},this.interval);
  }
  protected stopTimer():void {if(this.timer!==undefined){this.clock.clearTimeout(this.timer);this.timer=undefined;}for(const timer of this.deadlines)this.clock.clearTimeout(timer);this.deadlines.clear();for(const timer of this.liveness)this.clock.clearTimeout(timer);this.liveness.clear();}
  protected scheduleLiveness():void {
    for(const timer of this.liveness)this.clock.clearTimeout(timer);this.liveness.clear();
    for(const delay of (this.state==='open'?[this.pingAfter,this.lossAfter]:[this.lossAfter])){
      const timer=this.clock.setTimeout(()=>{this.liveness.delete(timer);if(this.state==='open'||this.state==='preparing')this.advance();},delay);
      this.liveness.add(timer);
    }
  }
  protected dispatch(event:Event):Result {
    const receipt=this.core.state==='open'&&'socket' in event&&event.socket===this.core.socket&&['ready','ack','data','ping','pong','resume','reject','peerClose'].includes(event.type);
    const r=this.core.dispatch(event);this.process(r);
    if(receipt&&this.core.state==='open')this.scheduleLiveness();
    return r;
  }
  protected receive(id:string,frame:Wire):void {
    if(frame.type==='ready')this.dispatch({type:'ready',socket:id});
    else if(frame.type==='data')this.dispatch({type:'data',socket:id,seq:frame.seq,data:frame.data});
    else if(frame.type==='ack')this.dispatch({type:'ack',socket:id,seq:frame.seq});
    else if(frame.type==='ping'||frame.type==='pong')this.dispatch({type:frame.type,socket:id});
    else if(frame.type==='close')this.dispatch({type:'peerClose',socket:id});
    if(this.core.state==='open'&&this.core.socket===id)this.scheduleLiveness();
    if((frame.type==='data'||frame.type==='ping')&&this.core.state==='open'&&this.core.socket===id){
      const timer=this.clock.setTimeout(()=>{this.deadlines.delete(timer);if(this.core.socket===id)this.advance();},1000);this.deadlines.add(timer);
    }
  }
  send(data:Json):Result {this.advance();return this.dispatch({type:'send',data});}
  close():Result {this.advance();const id=this.core.socket;const r=this.dispatch({type:'close'});if(id){const socket=this.sockets.get(id);this.sockets.delete(id);socket?.close();}return r;}
}
