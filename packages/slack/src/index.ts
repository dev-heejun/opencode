import { App } from "@slack/bolt"
import { createOpencode, type ToolPart } from "@opencode-ai/sdk"

const LOG_LEVEL = process.env.LOG_LEVEL || "info"

const logLevels: Record<string, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
  none: 4,
}

const currentLevel = logLevels[LOG_LEVEL] ?? 1

const log = {
  debug: (...args: any[]) => currentLevel <= 0 && console.log("🔍", ...args),
  info: (...args: any[]) => currentLevel <= 1 && console.log("ℹ️", ...args),
  warn: (...args: any[]) => currentLevel <= 2 && console.warn("⚠️", ...args),
  error: (...args: any[]) => currentLevel <= 3 && console.error("❌", ...args),
}

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  signingSecret: process.env.SLACK_SIGNING_SECRET,
  socketMode: true,
  appToken: process.env.SLACK_APP_TOKEN,
})

const OPENCODE_AGENT = process.env.OPENCODE_AGENT || "router"
const OPENCODE_MODEL = process.env.OPENCODE_MODEL

const parseModel = (modelStr?: string) => {
  if (!modelStr) return undefined
  const [providerID, modelID] = modelStr.split('/')
  if (!providerID || !modelID) return undefined
  return { providerID, modelID }
}

const modelConfig = parseModel(OPENCODE_MODEL)

// 허용 목록 (비어있으면 전체 허용)
const ALLOWED_CHANNELS = process.env.ALLOWED_CHANNELS
  ? new Set(process.env.ALLOWED_CHANNELS.split(",").map(s => s.trim()).filter(Boolean))
  : null
const ALLOWED_USER_IDS = process.env.ALLOWED_USER_IDS
  ? new Set(process.env.ALLOWED_USER_IDS.split(",").map(s => s.trim()).filter(Boolean))
  : null

// DM 허용 여부:
// - ALLOWED_USER_IDS가 설정된 경우: 해당 유저만 허용
// - ALLOWED_USER_IDS 미설정 + ALLOWED_CHANNELS 설정된 경우: DM 전체 차단 (채널 제한이 걸려있으면 DM도 차단)
// - 둘 다 미설정: 전체 허용
function isDMAllowed(userId: string): boolean {
  if (ALLOWED_USER_IDS) return ALLOWED_USER_IDS.has(userId)
  if (ALLOWED_CHANNELS) return false
  return true
}

// 채널: 채널 허용 목록만 체크
function isChannelAllowed(channel: string): boolean {
  if (ALLOWED_CHANNELS && !ALLOWED_CHANNELS.has(channel)) return false
  return true
}

log.info("Bot configuration:")
log.info("- Agent:", OPENCODE_AGENT, "| Model:", OPENCODE_MODEL || "(default)")
log.info("- Log level:", LOG_LEVEL)
log.info("- Working directory:", process.cwd())
log.info("- Allowed channels:", ALLOWED_CHANNELS ? [...ALLOWED_CHANNELS].join(", ") : "(all)")
log.info("- Allowed users:", ALLOWED_USER_IDS ? [...ALLOWED_USER_IDS].join(", ") : "(all)")

log.info("Starting opencode server...")
const opencode = await createOpencode({ port: 0 })
log.info("Opencode server ready, URL:", opencode.server.url)

const authResult = await app.client.auth.test()
const botUserId = authResult.user_id || ""
log.info("Bot user ID:", botUserId)

const sessions = new Map<string, { sessionId: string; channel: string; thread: string }>()
const sentMessages = new Set<string>()
const userNames = new Map<string, string>()
const processedEvents = new Set<string>()

async function getUserName(userId: string): Promise<string> {
  const cached = userNames.get(userId)
  if (cached) return cached
  try {
    const result = await app.client.users.info({ user: userId })
    const name = result.user?.profile?.display_name || result.user?.real_name || userId
    userNames.set(userId, name)
    return name
  } catch {
    return userId
  }
}

process.on("uncaughtException", (err) => {
  log.error("Uncaught exception:", err)
})

process.on("unhandledRejection", (reason, promise) => {
  log.error("Unhandled rejection:", reason)
})

function findSession(sessionId: string) {
  for (const [, session] of sessions.entries()) {
    if (session.sessionId === sessionId) return session
  }
  return undefined
}

// 이벤트 스트림
async function startEventStream() {
  while (true) {
    try {
      log.info("Subscribing to opencode events...")
      const events = await opencode.client.event.subscribe()

      for await (const event of events.stream) {
        if (event.type === "server.heartbeat") continue
        if (event.type === "message.part.delta") continue

        log.debug("Event:", event.type, JSON.stringify(event.properties || {}).substring(0, 300))

        if (event.type === "session.error") {
          const props = event.properties as any
          const session = findSession(props.sessionID)
          log.error("Session error:", JSON.stringify(props.error, null, 2))
          if (session) {
            await app.client.chat.postMessage({
              channel: session.channel,
              thread_ts: session.thread,
              text: `❌ 오류: ${props.error?.data?.message || "알 수 없는 오류"}`,
            }).catch(() => {})
          }
        }

        if (event.type === "message.part.updated") {
          const part = event.properties.part
          const session = findSession(part.sessionID)
          if (session && part.type === "tool") {
            await handleToolUpdate(part, session.channel, session.thread)
          }
        }

        if (event.type === "message.updated") {
          const msg = event.properties.info as any
          if (msg.role !== "assistant") continue
          if (!msg.finish || msg.finish !== "stop") continue
          if (sentMessages.has(msg.id)) continue

          sentMessages.add(msg.id)
          log.info("Assistant message completed:", msg.id)

          const session = findSession(msg.sessionID)
          if (!session) continue

          const messageResult = await opencode.client.session.message({
            path: { id: msg.sessionID, messageID: msg.id }
          })

          if (messageResult.error) {
            log.error("Error fetching message:", messageResult.error)
            continue
          }

          const data = messageResult.data as any
          if (!data) continue

          const parts = data.parts || []
          const info = data.info || {}
          const textParts = parts.filter((p: any) => p.type === "text")
          const responseText = textParts.map((p: any) => p.text).join("\n")

          log.debug("Response text length:", responseText?.length, "Agent:", info.agent)

          if (responseText && responseText.trim()) {
            await app.client.chat.postMessage({
              channel: session.channel,
              thread_ts: session.thread,
              text: responseText,
            }).catch((err) => log.error("Failed to send:", err))
            log.info("Sent response to Slack [", info.agent, "]")
          }
        }
      }
    } catch (err) {
      log.error("Event stream error, reconnecting in 3s:", err)
      await new Promise(resolve => setTimeout(resolve, 3000))
    }
  }
}

async function handleToolUpdate(part: ToolPart, channel: string, thread: string) {
  // task 진행 알림 비활성화
}

async function handleMessage(text: string, channel: string, thread: string, userId: string, say: (opts: any) => Promise<void>) {
   const name = await getUserName(userId)
  const fullText = `[${name}] ${text}`

  const sessionKey = `${channel}-${thread}`
  let session = sessions.get(sessionKey)

  if (!session) {
    const createResult = await opencode.client.session.create({
      body: { title: `Slack thread ${thread}` },
    })

    if (createResult.error) {
      log.error("Failed to create session:", createResult.error)
      await say({ text: "세션 생성에 실패했습니다.", thread_ts: thread })
      return
    }

    log.info("Created session:", createResult.data.id)
    session = { sessionId: createResult.data.id, channel, thread }
    sessions.set(sessionKey, session)
  }

  // promptAsync - fire and forget, 응답은 이벤트 스트림으로 수신
  const url = `${opencode.server.url}/session/${session.sessionId}/prompt_async`
  log.debug("Calling prompt_async:", url)

  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      agent: OPENCODE_AGENT,
      model: modelConfig,
      parts: [{ type: "text", text: fullText }]
    }),
  })

  log.debug("prompt_async status:", resp.status)

  if (!resp.ok) {
    const errText = await resp.text()
    log.error("prompt_async error:", resp.status, errText)
    await say({ text: "메시지 전송에 실패했습니다.", thread_ts: thread })
  }
}

app.message(async ({ message, say }) => {
    try {
      if (message.subtype || !("text" in message) || !message.text) return
      
      const msgTs = (message as any).ts
      if (processedEvents.has(msgTs)) return

      const channel = message.channel
      const thread = (message as any).thread_ts || message.ts
      const userId = (message as any).user
      const isDM = (message as any).channel_type === "im"

      // 채널 멘션: processedEvents에 추가하지 않고 즉시 리턴 → app_mention 핸들러가 세션 생성 후 처리
      if (!isDM && botUserId && message.text.includes(`<@${botUserId}>`)) {
        log.debug("Skipping bot mention in app.message (channel) - app_mention will handle")
        return
      }

      // 이 핸들러가 처리할 메시지 → 중복 방지를 위해 processedEvents에 추가
      processedEvents.add(msgTs)

      log.debug("Raw message text:", message.text.substring(0, 100), "| botUserId:", botUserId)

      if (isDM) {
        // DM: 유저 허용 목록만 체크
        if (!isDMAllowed(userId)) {
          log.debug("Blocked DM - user not in allowed list. user:", userId)
          await say({ text: "해당 채널에서는 지원되지 않는 기능입니다. 담당자에게 문의해 주세요.", thread_ts: thread }).catch(() => {})
          return
        }
        const text = message.text.replace(/<@[A-Z0-9]+>/g, "").trim()
        if (!text) return
        await handleMessage(text, channel, thread, userId, say)
        return
      }

      // 채널: 채널 허용 목록 체크
      if (!isChannelAllowed(channel)) {
        log.debug("Blocked channel - not in allowed list. channel:", channel)
        await say({ text: "해당 채널에서는 지원되지 않는 기능입니다. 담당자에게 문의해 주세요.", thread_ts: thread }).catch(() => {})
        return
      }

      // 채널: 스레드 답글이고, 해당 스레드에 이미 세션이 있으면 처리 (멘션 없이도)
      if ((message as any).thread_ts) {
        const sessionKey = `${channel}-${(message as any).thread_ts}`
        log.info("Thread reply received. sessionKey:", sessionKey, "| has session:", sessions.has(sessionKey), "| known sessions:", [...sessions.keys()])
        if (sessions.has(sessionKey)) {
          const text = message.text.replace(/<@[A-Z0-9]+>/g, "").trim()
          if (!text) return
          await handleMessage(text, channel, (message as any).thread_ts, userId, say)
          return
        }
      }
    } catch (err) {
      log.error("Error processing message:", err)
      try {
        await say({
          text: "오류가 발생했습니다.",
          thread_ts: (message as any).thread_ts || (message as any).ts,
        })
      } catch {}
    }
  })

app.event("app_mention", async ({ event, say }) => {
   try {
     const eventTs = event.ts
     if (processedEvents.has(eventTs)) return
     processedEvents.add(eventTs)
     
     const text = (event as any).text || ""
     const cleanText = text.replace(/<@[A-Z0-9]+>/g, "").trim()

     const channel = event.channel
     const thread = (event as any).thread_ts || event.ts
     const userId = event.user

     // DM 멘션: 유저 허용 목록만 체크 / 채널 멘션: 채널 허용 목록 체크
     const isDM = channel.startsWith("D")
     if (isDM) {
       if (!isDMAllowed(userId)) {
         log.debug("Blocked DM mention - user not in allowed list. user:", userId)
         await say({ text: "해당 채널에서는 지원되지 않는 기능입니다. 담당자에게 문의해 주세요.", thread_ts: thread }).catch(() => {})
         return
       }
     } else {
       if (!isChannelAllowed(channel)) {
         log.debug("Blocked channel mention - not in allowed list. channel:", channel)
         await say({ text: "해당 채널에서는 지원되지 않는 기능입니다. 담당자에게 문의해 주세요.", thread_ts: thread }).catch(() => {})
         return
       }
     }

     if (!cleanText) return
     log.info("Processing mention:", cleanText.substring(0, 50))

     await handleMessage(cleanText, channel, thread, userId, say)
   } catch (err) {
     log.error("Error processing mention:", err)
     try {
       await say({
         text: "오류가 발생했습니다.",
         thread_ts: (event as any).thread_ts || (event as any).ts,
       })
     } catch {}
   }
 })

await app.start()
log.info("Slack bot is running!")

startEventStream().catch((err) => log.error("Event stream fatal error:", err))
