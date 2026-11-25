import {Core, type Json} from './core.js';
import {Endpoint, systemClock, parseWire, type Callbacks, type SocketLike, type Timing, type Wire} from './transport.js';

export interface ClientOptions extends Timing,Callbacks {
  socketFactory?:(url:string)=>SocketLike;
  retryInitial?:number;
  retryMax?:number;
  random?:()=>number;
}
export class Client extends Endpoint {
  private readonly url:string;
  private readonly options:ClientOptions;
  private serial=0;
  private retries=0;
  private retryTimer?:unknown;
  private attemptTimer?:unknown;
  private started=false;
  private requested=false;
  constructor(url:string,options:ClientOptions={}) {
    const clock=options.clock??systemClock;
    super(new Core({role:'client',identifier:'',now:clock.now(),...options}),options,options);
    this.url=url;this.options=options;
  }
  connect():void {
    if(this.started)return;
    this.started=true;this.callbacks.onState?.('preparing');this.dial();
  }
  protected override advance():void {if(this.state==='preparing'&&!this.requested)return;super.advance();}
  protected retry():void {
    if(this.retryTimer!==undefined||this.state!=='disconnected')return;
    const maximum=Math.min(10000,this.options.retryMax??10000);
    const base=Math.min(maximum,(this.options.retryInitial??500)*2**Math.min(this.retries++,30));
    const random=this.options.random??Math.random;
    const delay=base+(Math.min(maximum,base*1.1)-base)*Math.max(0,Math.min(1,random()));
    this.retryTimer=this.clock.setTimeout(()=>{this.retryTimer=undefined;this.advance();if(this.state==='disconnected')this.dial();},delay);
  }
  private dial():void {
    const id=`client-${++this.serial}`;
    if(this.state==='preparing')this.core.socket=id;
    let socket:SocketLike;
    try{socket=(this.options.socketFactory??(url=>new WebSocket(url)))(this.url);}
    catch{if(this.state==='preparing')this.process(this.core.dispatch({type:'loss',socket:this.core.socket??''}));else this.retry();return;}
    this.sockets.set(id,socket);
    if(this.state!=='preparing'){
      this.dispatch({type:'attempt',socket:id});if(this.core.socket!==id){this.sockets.delete(id);socket.close();return;}
      this.attemptTimer=this.clock.setTimeout(()=>{this.attemptTimer=undefined;if(this.core.socket===id)this.advance();},10000);
    }
    socket.addEventListener('open',()=>{
      if(this.core.socket!==id)return;
      const hello:Wire=this.state==='preparing'?{type:'hello'}:{type:'hello',identifier:this.core.record.identifier,seq:this.core.record.received};
      try{
        if(this.state==='preparing'){this.core.beginPreparing(this.clock.now());this.requested=true;}
        socket.send(JSON.stringify(hello));
        this.runTimer();
      }catch{this.dispatch({type:'loss',socket:id});}
    });
    socket.addEventListener('message',(event:any)=>{
      if(this.core.socket!==id)return;
      this.advance();if(this.core.socket!==id)return;
      const frame=parseWire(event.data);if(!frame)return;
      if(frame.type==='welcome'){
        if(this.state==='preparing'&&!frame.resumed){
          this.core.record.identifier=frame.identifier;
          try{socket.send(JSON.stringify({type:'ready'}));}catch{this.dispatch({type:'loss',socket:id});return;}
          this.retries=0;this.dispatch({type:'ready',socket:id});
        }
        else if(this.state==='disconnected'&&frame.resumed){this.clearAttemptTimer();this.retries=0;this.dispatch({type:'resume',socket:id,seq:frame.seq});}
        else if(this.state==='open')this.dispatch({type:'ready',socket:id});
      }else if(frame.type==='reject'){this.clearAttemptTimer();this.dispatch({type:'reject',socket:id});}
      else this.receive(id,frame);
    });
    const lost=()=>{this.sockets.delete(id);if(this.core.socket===id){this.clearAttemptTimer();this.advance();this.dispatch({type:'loss',socket:id});}};
    socket.addEventListener('error',lost);socket.addEventListener('close',lost);
  }
  override close() {
    this.clearAttemptTimer();
    if(this.retryTimer!==undefined){this.clock.clearTimeout(this.retryTimer);this.retryTimer=undefined;}
    return super.close();
  }
  private clearAttemptTimer():void {if(this.attemptTimer!==undefined){this.clock.clearTimeout(this.attemptTimer);this.attemptTimer=undefined;}}
}
export function connect(url:string,options:ClientOptions={}):Client {const c=new Client(url,options);c.connect();return c;}
export type {Json};
