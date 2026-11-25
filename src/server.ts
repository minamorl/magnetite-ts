import {Core, issueIdentifier, type Json, type Result} from './core.js';
import {Endpoint, parseWire, systemClock, type Callbacks, type SocketLike, type Timing} from './transport.js';

export interface ServerOptions extends Timing {
  randomBytes?:(length:number)=>Uint8Array;
  onConnection?:(connection:ServerConnection)=>void;
}
export class ServerConnection extends Endpoint {
  readonly identifier:string;
  private onTerminal:(connection:ServerConnection,reason:string)=>void;
  constructor(core:Core, options:ServerOptions, onTerminal:(connection:ServerConnection,reason:string)=>void) {
    super(core,options,{});this.identifier=core.record.identifier;this.onTerminal=onTerminal;
  }
  setCallbacks(callbacks:Callbacks):void {this.callbacks=callbacks;}
  protected retry():void {}
  protected override effect(effect:Parameters<Endpoint['effect']>[0]):void {
    super.effect(effect);
    if(effect.type==='ended')this.onTerminal(this,effect.reason);
  }
  attach(id:string,socket:SocketLike):void {this.sockets.set(id,socket);this.runTimer();}
  initial(id:string):void {this.dispatch({type:'ready',socket:id});}
  resume(id:string,seq:number):Result {return this.core.dispatch({type:'resume',socket:id,seq});}
  sync():void {this.advance();}
  apply(result:Result):void {this.process(result);}
  get received():number{return this.core.record.received;}
  frame(id:string,frame:Parameters<Endpoint['receive']>[1]):void {this.advance();this.receive(id,frame);}
  loss(id:string):void {
    this.sockets.delete(id);
    if(this.core.socket===id){this.advance();const before=this.state;this.dispatch({type:'loss',socket:id});if(before==='open'&&this.state==='disconnected')this.callbacks.onState?.('disconnected');}
  }
  successorEffects():void {this.process({effects:this.core.successorEffects()});}
  announceOpen():void {this.callbacks.onState?.('open');}
}
export class Server {
  private readonly options:ServerOptions;
  private readonly sessions=new Map<string,ServerConnection>();
  private serial=0;
  constructor(options:ServerOptions={}) {this.options=options;}
  accept(socket:SocketLike):void {
    const id=`server-${++this.serial}`;
    let session:ServerConnection|undefined;
    const send=(frame:unknown)=>{if(socket.readyState===1)socket.send(JSON.stringify(frame));};
    socket.addEventListener('message',(event:any)=>{
      const frame=parseWire(event.data);if(!frame)return;
      if(!session) {
        if(frame.type!=='hello')return;
        if(frame.identifier===undefined){
          const clock=this.options.clock??systemClock;
          const random=this.options.randomBytes??((n:number)=>globalThis.crypto.getRandomValues(new Uint8Array(n)));
          const identifier=issueIdentifier(clock.now(),random);
          const core=new Core({role:'server',identifier,now:clock.now(),socket:id,...this.options});
          session=this.create(core);session.attach(id,socket);
          send({type:'welcome',identifier,seq:0,resumed:false});
          this.options.onConnection?.(session);session.initial(id);
        }else {
          const prior=this.sessions.get(frame.identifier);
          if(!prior){send({type:'reject',reason:'unknown_identifier'});return;}
          prior.sync();
          if(prior.state==='closed'){send({type:'reject',reason:'unknown_identifier'});return;}
          const seq=frame.seq??NaN;const received=prior.received;
          const result=prior.resume(id,seq);
          const reject=result.effects.find(e=>e.type==='reject');
          if(reject&&reject.type==='reject'){prior.apply(result);send({type:'reject',reason:reject.reason});return;}
          const transfer=result.effects.find(e=>e.type==='transfer');
          if(transfer&&transfer.type==='transfer'){
            prior.apply(result);
            const clock=this.options.clock??systemClock;
            const core=new Core({role:'server',identifier:transfer.record.identifier,record:transfer.record,now:clock.now(),socket:id,successor:true,...this.options});
            session=this.create(core);session.attach(id,socket);
            send({type:'welcome',identifier:session.identifier,seq:received,resumed:true});
            session.successorEffects();this.options.onConnection?.(session);session.announceOpen();
          }else {
            session=prior;session.attach(id,socket);
            send({type:'welcome',identifier:session.identifier,seq:received,resumed:true});session.apply(result);
          }
        }
      }else session.frame(id,frame);
    });
    const lost=()=>session?.loss(id);
    socket.addEventListener('close',lost);socket.addEventListener('error',lost);
  }
  private create(core:Core):ServerConnection {
    const connection=new ServerConnection(core,this.options,(ended,reason)=>{
      if(reason!=='replaced'&&this.sessions.get(ended.identifier)===ended)this.sessions.delete(ended.identifier);
    });
    this.sessions.set(connection.identifier,connection);return connection;
  }
}
export type {Json};
