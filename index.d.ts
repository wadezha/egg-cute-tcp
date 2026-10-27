
import { Context } from "egg";
import { PeerCertificate } from 'tls';

export declare class Chain {
  ctx: Context;
  protected constructor(ctx: Context);
  protected handle(session: TcpSession, message: any, sessionHub: TcpSessionHub): Promise<{ message?: any; break?: boolean } | void>;
}

export declare class Parser {
  ctx: Context;
  protected constructor(ctx: Context);
  protected encode(...args: any[]): Promise<any>;
  protected decode(...args: any[]): Promise<any>;
}

declare class TcpPubSub {
  /** RabbitMQ only: resubscribe after egg-rabbitmq re-establishes its connection. */
  restart?(): Promise<void>;
}

declare class TcpChannel {
  closed: boolean;
  /** holds the unfinished frame, see "Sticky and split packets" */
  frameBuf: Buffer | null;
}

export declare class TcpSession {
  readonly destroyed: boolean;
  readonly sessionId: number;
  sessionKey?: string;
  readonly clientIp: string;
  readonly clientPort: number | null;
  readonly connectTime: string | null;
  readonly registerTime: string | null;
  channel: TcpChannel | null;
  toString(): string;
}

declare class TcpClient {
  sessionHub: TcpSessionHub;
  parser: any;
  chain: any;
}

export declare class TcpSessionHub {
  register(session: TcpSession): Promise<void>;
  getSession(sessionKey: string): TcpSession | null;
  getKeys(): Array<string>;
  kickout(sessionKey: string): Promise<boolean>;
  send(sessionKey: string, message: any): Promise<boolean>;
  getPeerCertificate(sessionId: number): PeerCertificate | null;
}

/**
 * `enable` turns TLS on, every other option is passed to Node's tls.createServer().
 * key / cert / ca / pfx may be file paths (absolute or relative to the app base dir), PEM strings or Buffers.
 */
interface TlsConfig {
  enable: boolean;
  [option: string]: any;
}

interface ClientConfig {
  port: number;
  protocol: string;
  enableDispatchQueue: boolean;
  idleTimeout: number;
  keepAlive: number;
  sessionRedisClient: string;
  sessionKeyCacheKey: string;
  sessionIdCacheKey: string;

  parserPath: string;
  chainPath: string;
  inChain: Array<string>;
  outChain: Array<string>;

  tls?: TlsConfig;
}

interface EggClientConfig {
  app?: boolean;
  pubSubType: string;
  pubSubChannel: string;
  pubSubClient: string;
  eventSend: string;
  eventKickout: string;
  client?: ClientConfig;
  clients?: Record<string, ClientConfig>;
}

declare module 'egg' {
  interface Application {
    tcp: TcpClient & Singleton<TcpClient>;
    tcpPubSub?: TcpPubSub;
  }

  interface EggAppConfig {
    tcp: EggClientConfig;
  }
}
