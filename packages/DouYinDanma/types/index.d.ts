import { TypedEmitter } from "tiny-typed-emitter";
import type {
  ChatMessage,
  MemberMessage,
  LikeMessage,
  SocialMessage,
  GiftMessage,
  RoomUserSeqMessage,
  RoomStatsMessage,
  RoomRankMessage,
  Message,
  PrivilegeScreenChatMessage,
  ScreenChatMessage,
} from "../types/types.js";
import WebSocket from "ws";

interface Events {
  init: (url: string) => void;
  open: () => void;
  close: () => void;
  reconnect: (count: number) => void;
  heartbeat: () => void;
  error: (error: Error) => void;
  chat: (message: ChatMessage) => void;
  member: (message: MemberMessage) => void;
  like: (message: LikeMessage) => void;
  social: (message: SocialMessage) => void;
  gift: (message: GiftMessage) => void;
  roomUserSeq: (message: RoomUserSeqMessage) => void;
  roomStats: (message: RoomStatsMessage) => void;
  roomRank: (message: RoomRankMessage) => void;
  message: (message: Message) => void;
  privilegeScreenChat: (message: PrivilegeScreenChatMessage) => void;
  screenChat: (message: ScreenChatMessage) => void;
}
declare class DouYinDanmaClient extends TypedEmitter<Events> {
  private ws: WebSocket | undefined;
  constructor(
    roomId: string,
    options?: {
      autoStart?: boolean;
      autoReconnect?: number;
      heartbeatInterval?: number;
      reconnectInterval?: number;
      cookie?: string;
      timeoutInterval?: number;
      host?: string;
      /** ws（默认，WSS 推送，失败后退回 fetch） | fetch（im/fetch 长轮询） */
      mode?: "ws" | "fetch";
      /** ws 握手连续失败多少次后切 fetch，默认 20 */
      maxWsFailures?: number;
      /** ws 握手超时（毫秒），默认 10000 */
      wsConnectTimeout?: number;
    },
  );
  connect(): Promise<void>;
}
export default DouYinDanmaClient;