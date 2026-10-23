
import { Context } from "egg";

export declare class Chain {
  ctx: Context;
  protected constructor(ctx: Context);
  protected handle(...args: any[]): Promise<any>;
}

export declare class Parser {
  ctx: Context;
  protected constructor(ctx: Context);
  protected encode(...args: any[]): Promise<any>;
  protected decode(...args: any[]): Promise<any>;
}

declare class TcpSession {
  destroyed: boolean;
  sessionId: number;
  sessionKey?: string;
}

declare class TcpClient {
  sessionHub: TcpSessionHub;
  parser: any;
  chain: any;
}

declare class TcpSessionHub {
  register(session: TcpSession): Promise<void>;
  getSession(sessionKey: string): TcpSession | null;
  getKeys(): Array<string>;
  send(sessionKey: string, message: any): Promise<boolean>;
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
  }

  interface EggAppConfig {
    tcp: EggClientConfig;
  }
}
