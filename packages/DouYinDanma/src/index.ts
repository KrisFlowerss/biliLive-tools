import WebSocket from "ws";
import { TypedEmitter } from "tiny-typed-emitter";

import { decompressGzip, getXMsStub, getSignature, getUserUniqueId } from "./utils.js";
import protobuf from "./proto.js";
import { getCookie } from "./api.js";
import { ABogus } from "./abogus.js";

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

const DOUYIN_WEB_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36";
const DOUYIN_WS_ORIGIN = "https://live.douyin.com";

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
  privilegeScreenChat: (message: PrivilegeScreenChatMessage) => void;
  screenChat: (message: ScreenChatMessage) => void;
  message: (message: Message) => void;
}

class DouYinDanmaClient extends TypedEmitter<Events> {
  private ws!: WebSocket;
  private roomId: string;
  private autoStart: boolean;
  private autoReconnect: number;
  private reconnectAttempts: number;
  private reconnectInterval: number;
  private cookie?: string;
  private timeoutInterval: number;
  private lastMessageTime: number;
  private timeoutTimer!: NodeJS.Timeout;
  private isTimeoutCheckRunning: boolean = false;
  private isReconnecting: boolean = false;
  private host: string;
  /** 是否已停止 */
  private isStopped: boolean = true;
  /** 当前通道：ws（WSS 推送） | fetch（im/fetch 长轮询） */
  private mode: "ws" | "fetch";
  /** ws 握手连续失败次数，达到 maxWsFailures 后永久退回 fetch */
  private wsConnectFailures: number = 0;
  private readonly maxWsFailures: number;
  private readonly wsConnectTimeout: number;
  /** 握手阶段挂起的 Promise，open/close/超时只结算一次 */
  private openResolve: ((ok: boolean) => void) | null = null;
  private wsOpened: boolean = false;
  private heartbeatInterval: number;
  private heartbeatTimer!: NodeJS.Timeout;
  private isHeartbeatRunning: boolean = false;

  constructor(
    roomId: string,
    options: {
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
    } = {},
  ) {
    super();
    this.roomId = roomId;
    this.autoStart = options.autoStart ?? false;
    this.autoReconnect = options.autoReconnect ?? 10;
    this.reconnectAttempts = 0;
    this.reconnectInterval = options.reconnectInterval ?? 10000;
    this.cookie = options.cookie;
    this.timeoutInterval = options.timeoutInterval ?? 100000; // 默认100秒
    this.lastMessageTime = Date.now();
    this.host = options.host ?? "webcast100-ws-web-hl.douyin.com";
    this.mode = options.mode === "fetch" ? "fetch" : "ws";
    this.maxWsFailures = options.maxWsFailures ?? 20;
    this.wsConnectTimeout = options.wsConnectTimeout ?? 10000;
    this.heartbeatInterval = options.heartbeatInterval ?? 10000;

    if (this.autoStart) {
      this.connect();
    }
  }

  async connect() {
    this.isStopped = false;

    // fetch 模式：直接走 im/fetch 长轮询
    if (this.mode === "fetch") {
      await this.connectFetch();
      return;
    }

    // ws 模式（默认）：webmssdk 签名 + WSS 推送，握手连续失败多次后永久退回 fetch
    const ok = await this.connectWebSocket();
    if (this.isStopped || ok) return;

    this.wsConnectFailures += 1;
    if (this.wsConnectFailures >= this.maxWsFailures) {
      this.mode = "fetch";
    }
    // ws 握手失败要重试到 maxWsFailures 次，不能受 autoReconnect(默认10) 提前掐断
    this.reconnect(Math.max(this.autoReconnect, this.maxWsFailures));
  }

  /** WSS 推送模式：webmssdk 签名建立 /webcast/im/push/v2/ 连接 */
  private async connectWebSocket(): Promise<boolean> {
    try {
      const url = await this.getWsInfo(this.roomId);
      if (!url) {
        throw new Error("获取抖音弹幕签名失败（webmssdk）");
      }
      this.emit("init", url);

      const cookies = this.cookie || (await getCookie());
      this.wsOpened = false;
      this.ws = new WebSocket(url, {
        headers: {
          Cookie: cookies,
          "User-Agent": DOUYIN_WEB_UA,
          Origin: DOUYIN_WS_ORIGIN,
          Referer: DOUYIN_WS_ORIGIN + "/",
        },
      });

      this.ws.on("open", () => {
        this.reconnectAttempts = 0;
        this.wsConnectFailures = 0;
        this.resolveOpen(true);
        this.emit("open");
        this.startHeartbeat();
        this.startTimeoutCheck();
      });
      this.ws.on("message", (data) => {
        this.lastMessageTime = Date.now();
        try {
          this.decode(data as Buffer);
        } catch (error) {
          this.emit("error", error as Error);
        }
      });
      this.ws.on("error", (error) => {
        this.emit("error", error as Error);
      });
      this.ws.on("close", () => {
        this.stopHeartbeat();
        this.stopTimeoutCheck();
        const wasOpened = this.wsOpened;
        this.resolveOpen(false);
        if (this.isStopped) return;
        this.emit("close");
        // 已成功连接后中途断开才自动重连；握手阶段失败由 connect() 兜底
        if (wasOpened) {
          this.reconnect();
        }
      });

      return await new Promise<boolean>((resolve) => {
        this.openResolve = resolve;
        setTimeout(() => this.resolveOpen(false), this.wsConnectTimeout);
      });
    } catch (error) {
      this.emit("error", error as Error);
      return false;
    }
  }

  /** 关闭连接握手阶段挂起的 Promise，只结算一次 */
  private resolveOpen(ok: boolean) {
    if (!this.openResolve) return;
    const resolve = this.openResolve;
    this.openResolve = null;
    if (ok) this.wsOpened = true;
    resolve(ok);
  }

  /** im/fetch 长轮询模式（a_bogus 签名） */
  private async connectFetch(): Promise<void> {
    this.emit("init", "webcast/im/fetch");
    const cookies = this.cookie || (await getCookie());
    const abogus = new ABogus();
    let cursor = "0";
    this.emit("open");
    this.startTimeoutCheck();
    try {
      while (!this.isStopped) {
        const uid = getUserUniqueId();
        const now = Date.now();
        const params = this.buildFetchParams(this.roomId, cursor, uid, now, DOUYIN_WEB_UA);
        const [query] = abogus.generateAbogus(params, "");
        const resp = await fetch(`https://live.douyin.com/webcast/im/fetch/?${query}`, {
          headers: {
            cookie: cookies,
            "User-Agent": DOUYIN_WEB_UA,
            Referer: "https://live.douyin.com/",
          },
        });
        const buf = new Uint8Array(await resp.arrayBuffer());
        const payloadPackage = (protobuf as any).douyin.Response.decode(buf);
        if (payloadPackage.cursor) cursor = payloadPackage.cursor;
        this.lastMessageTime = Date.now();
        this.processResponse(payloadPackage);
        const interval = Math.max(payloadPackage.fetchInterval || 0, 1000);
        await new Promise((r) => setTimeout(r, interval));
      }
    } catch (error) {
      if (!this.isStopped) {
        this.emit("error", error as Error);
        this.reconnect();
      }
    }
  }

  /** 构建 im/fetch 参数（a_bogus 签名 + resp_content_type=protobuf，对齐浏览器当前请求） */
  private buildFetchParams(
    roomId: string,
    cursor: string,
    uid: string,
    now: number,
    ua: string,
  ): string {
    // encodeURIComponent 不编码 !'()*，补上以对齐浏览器全量 %XX 编码
    const uaEnc = encodeURIComponent(ua).replace(/[!'()*]/g, (c) =>
      "%" + c.charCodeAt(0).toString(16).toUpperCase(),
    );
    return `resp_content_type=protobuf&did_rule=3&device_id=&app_name=douyin_web&endpoint=live_pc&support_wrds=1&user_unique_id=${uid}&identity=audience&need_persist_msg_count=15&insert_task_id=&live_reason=&room_id=${roomId}&version_code=180800&last_rtt=1655&live_id=1&aid=6383&fetch_rule=1&cursor=${cursor}&internal_ext=internal_src%3Apushserver%7Cfirst_req_ms%3A${now}%7Cseq%3A1%7Cwss_msg_type%3Ar%7Cwrds_v%3A0&device_platform=web&cookie_enabled=true&screen_width=1280&screen_height=800&browser_language=zh-CN&browser_platform=MacIntel&browser_name=Mozilla&browser_version=${uaEnc}&browser_online=true&tz_name=Asia%2FShanghai`;
  }

  /** 处理 Response protobuf 的 messagesList（对齐原 decode 的消息分发） */
  private processResponse(payloadPackage: any) {
    const douyin = (protobuf as any).douyin;
    const ChatMessage = douyin.ChatMessage;
    const RoomUserSeqMessage = douyin.RoomUserSeqMessage;
    const MemberMessage = douyin.MemberMessage;
    const GiftMessage = douyin.GiftMessage;
    const LikeMessage = douyin.LikeMessage;
    const SocialMessage = douyin.SocialMessage;
    const RoomStatsMessage = douyin.RoomStatsMessage;
    const RoomRankMessage = douyin.RoomRankMessage;
    const PrivilegeScreenChatMessage = douyin.PrivilegeScreenChatMessage;
    const ScreenChatMessage = douyin.ScreenChatMessage;
    for (const msg of payloadPackage.messagesList) {
      try {
        if (msg.method === "WebcastChatMessage") {
          const chatMessage = ChatMessage.decode(msg.payload);
          this.handleChatMessage(chatMessage.toJSON() as ChatMessage);
        } else if (msg.method === "WebcastMemberMessage") {
          const memberMessage = MemberMessage.decode(msg.payload);
          this.handleEnterRoomMessage(memberMessage.toJSON() as MemberMessage);
        } else if (msg.method === "WebcastGiftMessage") {
          const giftMessage = GiftMessage.decode(msg.payload);
          this.handleGiftMessage(giftMessage.toJSON() as GiftMessage);
        } else if (msg.method === "WebcastLikeMessage") {
          const message = LikeMessage.decode(msg.payload);
          this.handleLikeMessage(message.toJSON() as LikeMessage);
        } else if (msg.method === "WebcastSocialMessage") {
          const message = SocialMessage.decode(msg.payload);
          this.handleSocialMessage(message.toJSON() as SocialMessage);
        } else if (msg.method === "WebcastRoomUserSeqMessage") {
          const message = RoomUserSeqMessage.decode(msg.payload);
          this.handleRoomUserSeqMessage(message.toJSON() as RoomUserSeqMessage);
        } else if (msg.method === "WebcastRoomStatsMessage") {
          const message = RoomStatsMessage.decode(msg.payload);
          this.handleRoomStatsMessage(message.toJSON() as RoomStatsMessage);
        } else if (msg.method === "WebcastRoomRankMessage") {
          const message = RoomRankMessage.decode(msg.payload);
          this.handleRoomRankMessage(message.toJSON() as RoomRankMessage);
        } else if (msg.method === "WebcastPrivilegeScreenChatMessage") {
          const message = PrivilegeScreenChatMessage.decode(msg.payload);
          this.handlePrivilegeScreenChatMessage(message.toJSON() as PrivilegeScreenChatMessage);
        } else if (msg.method === "WebcastScreenChatMessage") {
          const message = ScreenChatMessage.decode(msg.payload);
          this.handleScreenChatMessage(message.toJSON() as ScreenChatMessage);
        } else {
          // WebcastRanklistHourEntranceMessage 等
        }
      } catch (e) {
        console.error("error:", e, msg);
      }
    }
  }

  send(data: any) {
    if (!this.ws) {
      return;
    }
    this.ws.send(data);
  }

  close() {
    this.isStopped = true;
    this.reconnectAttempts = this.autoReconnect;
    this.stopHeartbeat();
    this.stopTimeoutCheck();
    this.emit("close");

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.close();
    }
  }

  /** WSS 心跳，每 heartbeatInterval 毫秒发送一次 */
  private startHeartbeat() {
    if (this.isHeartbeatRunning) {
      return;
    }
    this.stopHeartbeat();
    this.isHeartbeatRunning = true;

    this.heartbeatTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.emit("heartbeat");
        this.send(":\x02hb");
      } else {
        console.log("连接未就绪，当前状态:", this.ws?.readyState ?? "no ws");
      }
    }, this.heartbeatInterval);
  }

  private stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.isHeartbeatRunning = false;
    }
  }

  private startTimeoutCheck() {
    if (this.isTimeoutCheckRunning) {
      return;
    }

    this.stopTimeoutCheck();
    this.isTimeoutCheckRunning = true;

    // 重置最后消息时间，给连接一些初始化时间
    this.lastMessageTime = Date.now();
    this.timeoutTimer = setInterval(() => {
      const now = Date.now();
      if (now - this.lastMessageTime > this.timeoutInterval) {
        console.log("No message received for too long, reconnecting...");
        // 在重连前重置时间，避免立即触发下一次重连
        this.lastMessageTime = now;
        this.reconnect();
      }
    }, 1000);
  }

  private stopTimeoutCheck() {
    if (this.timeoutTimer) {
      clearInterval(this.timeoutTimer);
      this.isTimeoutCheckRunning = false;
    }
  }

  private reconnect(maxAttempts: number = this.autoReconnect) {
    if (this.isReconnecting) {
      return;
    }

    this.stopHeartbeat();
    this.stopTimeoutCheck();

    if (this.reconnectAttempts < maxAttempts) {
      this.isReconnecting = true;
      this.reconnectAttempts++;
      setTimeout(() => {
        this.connect();
        this.isReconnecting = false;
        this.emit("reconnect", this.reconnectAttempts);
      }, this.reconnectInterval);
    }
  }

  async handleMessage() {}

  /**
   * 处理弹幕消息
   */
  async handleChatMessage(chatMessage: ChatMessage) {
    this.emit("chat", chatMessage);
    this.emit("message", chatMessage);
  }

  /**
   * 处理进入房间
   */
  async handleEnterRoomMessage(message: MemberMessage) {
    this.emit("member", message);
    this.emit("message", message);
  }

  /**
   * 处理礼物消息
   */
  async handleGiftMessage(message: GiftMessage) {
    this.emit("gift", message);
    this.emit("message", message);
  }

  /**
   * 处理点赞消息
   */
  async handleLikeMessage(message: LikeMessage) {
    this.emit("like", message);
    this.emit("message", message);
  }

  /**
   * 处理social消息
   */
  async handleSocialMessage(message: SocialMessage) {
    this.emit("social", message);
    this.emit("message", message);
  }

  /**
   * 处理RoomUserSeqMessage
   */
  async handleRoomUserSeqMessage(message: RoomUserSeqMessage) {
    this.emit("roomUserSeq", message);
    this.emit("message", message);
  }

  /**
   * 处理 WebcastRoomStatsMessage
   */
  async handleRoomStatsMessage(message: RoomStatsMessage) {
    this.emit("roomStats", message);
    this.emit("message", message);
  }

  /**
   * 处理 WebcastRoomRankMessage
   */
  async handleRoomRankMessage(message: RoomRankMessage) {
    this.emit("roomRank", message);
    this.emit("message", message);
  }

  async handlePrivilegeScreenChatMessage(message: PrivilegeScreenChatMessage) {
    this.emit("privilegeScreenChat", message);
    this.emit("message", message);
  }

  async handleScreenChatMessage(message: ScreenChatMessage) {
    this.emit("screenChat", message);
    this.emit("message", message);
  }

  /**
   * 处理其他消息
   */
  async handleOtherMessage(message: any) {
    this.emit("message", message);
  }

  async decode(data: Buffer) {
    // @ts-ignore
    const PushFrame = protobuf.douyin.PushFrame;
    // @ts-ignore
    const Response = protobuf.douyin.Response;
    // @ts-ignore
    const ChatMessage = protobuf.douyin.ChatMessage;
    // @ts-ignore
    const RoomUserSeqMessage = protobuf.douyin.RoomUserSeqMessage;
    // @ts-ignore
    const MemberMessage = protobuf.douyin.MemberMessage;
    // @ts-ignore
    const GiftMessage = protobuf.douyin.GiftMessage;
    // @ts-ignore
    const LikeMessage = protobuf.douyin.LikeMessage;
    // @ts-ignore
    const SocialMessage = protobuf.douyin.SocialMessage;
    // @ts-ignore
    const RoomStatsMessage = protobuf.douyin.RoomStatsMessage;
    // @ts-ignore
    const RoomRankMessage = protobuf.douyin.RoomRankMessage;
    // @ts-ignore
    const PrivilegeScreenChatMessage = protobuf.douyin.PrivilegeScreenChatMessage;
    // @ts-ignore
    const ScreenChatMessage = protobuf.douyin.ScreenChatMessage;
    const wssPackage = PushFrame.decode(data);

    // @ts-ignore
    const logId = wssPackage.logId;

    let decompressed;
    try {
      // @ts-ignore
      if (wssPackage.payload instanceof Buffer) {
        // @ts-ignore
        decompressed = await decompressGzip(wssPackage.payload);
      } else {
        return;
      }
    } catch (e) {
      this.emit("error", e as Error);
      return;
    }

    const payloadPackage = Response.decode(decompressed);

    let ack = null;
    // @ts-ignore
    if (payloadPackage.needAck) {
      const obj = PushFrame.create({
        logId: logId,
        // @ts-ignore
        payloadType: payloadPackage.internalExt,
      });
      ack = PushFrame.encode(obj).finish();
    }

    const msgs: any[] = [];
    // @ts-ignore
    for (const msg of payloadPackage.messagesList) {
      // const now = new Date();
      try {
        if (msg.method === "WebcastChatMessage") {
          const chatMessage = ChatMessage.decode(msg.payload);
          this.handleChatMessage(chatMessage.toJSON() as ChatMessage);
        } else if (msg.method === "WebcastMemberMessage") {
          const memberMessage = MemberMessage.decode(msg.payload);
          this.handleEnterRoomMessage(memberMessage.toJSON() as MemberMessage);
        } else if (msg.method === "WebcastGiftMessage") {
          const giftMessage = GiftMessage.decode(msg.payload);
          this.handleGiftMessage(giftMessage.toJSON() as GiftMessage);
        } else if (msg.method === "WebcastLikeMessage") {
          const message = LikeMessage.decode(msg.payload);
          this.handleLikeMessage(message.toJSON() as LikeMessage);
        } else if (msg.method === "WebcastSocialMessage") {
          const message = SocialMessage.decode(msg.payload);
          this.handleSocialMessage(message.toJSON() as SocialMessage);
        } else if (msg.method === "WebcastRoomUserSeqMessage") {
          const message = RoomUserSeqMessage.decode(msg.payload);
          this.handleRoomUserSeqMessage(message.toJSON() as RoomUserSeqMessage);
        } else if (msg.method === "WebcastRoomStatsMessage") {
          const message = RoomStatsMessage.decode(msg.payload);
          this.handleRoomStatsMessage(message.toJSON() as RoomStatsMessage);
        } else if (msg.method === "WebcastRoomRankMessage") {
          const message = RoomRankMessage.decode(msg.payload);
          this.handleRoomRankMessage(message.toJSON() as RoomRankMessage);
        } else if (msg.method === "WebcastPrivilegeScreenChatMessage") {
          const message = PrivilegeScreenChatMessage.decode(msg.payload);
          this.handlePrivilegeScreenChatMessage(message.toJSON() as PrivilegeScreenChatMessage);
        } else if (msg.method === "WebcastScreenChatMessage") {
          const message = ScreenChatMessage.decode(msg.payload);
          this.handleScreenChatMessage(message.toJSON() as ScreenChatMessage);
        } else {
          // WebcastRanklistHourEntranceMessage,WebcastInRoomBannerMessage,WebcastRoomStreamAdaptationMessage
        }
      } catch (e) {
        console.error("error:", e, msg);
      }
    }
    if (ack) {
      this.send(ack);
    }
    return [msgs, ack];
  }
  async getWsInfo(roomId: string): Promise<string | undefined> {
    const userUniqueId = getUserUniqueId();
    // const userUniqueId = "7877922945687137703";
    const versionCode = 180800;
    const webcastSdkVersion = "1.0.15";

    const sigParams = {
      live_id: "1",
      aid: "6383",
      version_code: versionCode,
      webcast_sdk_version: webcastSdkVersion,
      room_id: roomId,
      sub_room_id: "",
      sub_channel_id: "",
      did_rule: "3",
      user_unique_id: userUniqueId,
      device_platform: "web",
      device_type: "",
      ac: "",
      identity: "audience",
    };

    let signature: string;
    try {
      const m = getXMsStub(sigParams);
      signature = getSignature(m); // 这里应该获取签名
    } catch (e) {
      return;
    }

    const webcast5Params = {
      app_name: "douyin_web",
      room_id: roomId,
      compress: "gzip",
      version_code: String(versionCode),
      webcast_sdk_version: webcastSdkVersion,
      update_version_code: webcastSdkVersion,
      live_id: "1",
      did_rule: "3",
      user_unique_id: userUniqueId,
      identity: "audience",
      signature: signature.toString(),
      device_platform: "web",
      cookie_enabled: "true",
      screen_width: "1920",
      screen_height: "1080",
      browser_language: "zh-CN",
      browser_platform: "Win32",
      browser_name: "Mozilla",
      browser_version:
        "5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
      browser_online: "true",
      tz_name: "Etc/GMT-8",
      host: "https://live.douyin.com",
      aid: "6383",
      endpoint: "live_pc",
      support_wrds: "1",
      im_path: "/webcast/im/fetch/",
      need_persist_msg_count: "15",
      heartbeatDuration: "0",
    };

    const wssUrl = `wss://${this.host}/webcast/im/push/v2/?${new URLSearchParams(webcast5Params).toString()}`;
    return wssUrl;
  }
}

export default DouYinDanmaClient;
