import { App } from "@slack/bolt"
import { createOpencode, type ToolPart } from "@opencode-ai/sdk"

// 로그 레벨 설정: "debug" | "info" | "warn" | "error" | "none"
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

// Agent and Model configuration
const OPENCODE_AGENT = process.env.OPENCODE_AGENT || "router"
const OPENCODE_MODEL = process.env.OPENCODE_MODEL // e.g., "naver-aac/claude-sonnet-4-5"

// Parse model into providerID and modelID
const parseModel = (modelStr?: string) => {
  if (!modelStr) return undefined
  const [providerID, modelID] = modelStr.split('/')
  if (!providerID || !modelID) return undefined
  return { providerID, modelID }
}

const modelConfig = parseModel(OPENCODE_MODEL)

log.info("Bot configuration:")
log.debug("- Bot token present:", !!process.env.SLACK_BOT_TOKEN)
log.debug("- Signing secret present:", !!process.env.SLACK_SIGNING_SECRET)
log.debug("- App token present:", !!process.env.SLACK_APP_TOKEN)
log.info("- Agent:", OPENCODE_AGENT, "| Model:", OPENCODE_MODEL || "(default)")
log.info("- Log level:", LOG_LEVEL)

log.info("Starting opencode server...")
const opencode = await createOpencode({
  port: 0,
})
log.info("Opencode server ready")

const sessions = new Map<string, { client: any; server: any; sessionId: string; channel: string; thread: string }>()

// Track which messages we've already sent responses for
const sentMessages = new Set<string>()

// 전역 에러 핸들러 - 프로세스 중단 방지
process.on("uncaughtException", (err) => {
  log.error("Uncaught exception:", err)
})

process.on("unhandledRejection", (reason, promise) => {
  log.error("Unhandled rejection:", reason)
})

// 이벤트 스트림 처리
async function startEventStream() {
  while (true) {
    try {
      log.info("Subscribing to opencode events...")
      const events = await opencode.client.event.subscribe()
      
      for await (const event of events.stream) {
        try {
          await handleEvent(event)
        } catch (err) {
          log.error("Error handling event:", err)
          // 개별 이벤트 에러는 무시하고 계속 진행
        }
      }
    } catch (err) {
      log.error("Event stream error, reconnecting in 3s:", err)
      await new Promise(resolve => setTimeout(resolve, 3000))
    }
  }
}

async function handleEvent(event: any) {
  // Handle tool updates
  if (event.type === "message.part.updated") {
    const part = event.properties.part

    for (const [sessionKey, session] of sessions.entries()) {
      if (session.sessionId === part.sessionID) {
        if (part.type === "tool") {
          await handleToolUpdate(part, session.channel, session.thread)
        }
        break
      }
    }
  }

  // Handle completed assistant messages
  if (event.type === "message.updated") {
    const msg = event.properties.info

    // Only process completed assistant messages
    if (msg.role === "assistant" && (msg as any).finish === "stop" && !sentMessages.has(msg.id)) {
      log.debug("Assistant message completed:", msg.id)
      sentMessages.add(msg.id) // Mark as sent early to prevent duplicates

      // Find the session
      for (const [sessionKey, session] of sessions.entries()) {
        if (session.sessionId === msg.sessionID) {
          log.debug("Found session, fetching message parts...")

          // Fetch the full message with parts
          const messageResult = await opencode.client.session.message({
            path: { id: msg.sessionID, messageID: msg.id }
          })

          log.debug("Message result:", JSON.stringify(messageResult, null, 2).substring(0, 500))

          if (messageResult.error) {
            log.error("Error fetching message:", messageResult.error)
            break
          }

          if (messageResult.data) {
            const data = messageResult.data as any
            const parts = data.parts || []
            log.debug("Parts count:", parts.length, "Types:", parts.map((p: any) => p.type))

            // Extract agent info from data.info
            const info = data.info || {}
            const agentName = info.agent || "unknown"
            const modelID = info.modelID || ""
            const providerID = info.providerID || ""

            log.debug("Agent info:", { agentName, modelID, providerID })

            const textParts = parts.filter((p: any) => p.type === "text")
            const responseText = textParts.map((p: any) => p.text).join("\n")

            log.debug("Response text length:", responseText?.length)

            if (responseText && responseText.trim()) {
              // Format message with agent info header
              const agentHeader = `🤖 *${agentName}* | ${providerID}/${modelID}\n───────────────────\n`
              const formattedText = agentHeader + responseText

              log.debug("Sending to Slack...")
              await app.client.chat.postMessage({
                channel: session.channel,
                thread_ts: session.thread,
                text: formattedText,
                reply_broadcast: true, // Also show in channel
              }).catch((err) => log.error("Failed to send:", err))
              log.info("Sent response to Slack [", agentName, "]")
            } else {
              log.warn("No text content to send")
            }
          } else {
            log.warn("No data in message result")
          }
          break
        }
      }
    }
  }
}

async function handleToolUpdate(part: ToolPart, channel: string, thread: string) {
  if (part.state.status !== "completed") return
  const toolMessage = `*${part.tool}* - ${part.state.title}`
  await app.client.chat
    .postMessage({
      channel,
      thread_ts: thread,
      text: toolMessage,
    })
    .catch(() => {})
}

app.use(async ({ next, context }) => {
  log.debug("Raw Slack event:", JSON.stringify(context, null, 2))
  await next()
})

app.message(async ({ message, say }) => {
  try {
    log.debug("Received message event:", JSON.stringify(message, null, 2))

    if (message.subtype || !("text" in message) || !message.text) {
      log.debug("Skipping message - no text or has subtype")
      return
    }

    log.info("Processing message:", message.text.substring(0, 50) + (message.text.length > 50 ? "..." : ""))

    const channel = message.channel
    const thread = (message as any).thread_ts || message.ts
    const sessionKey = `${channel}-${thread}`

    let session = sessions.get(sessionKey)

    if (!session) {
      log.debug("Creating new opencode session...")
      const { client, server } = opencode

      const createResult = await client.session.create({
        body: { title: `Slack thread ${thread}` },
      })

      if (createResult.error) {
        log.error("Failed to create session:", createResult.error)
        await say({
          text: "Sorry, I had trouble creating a session. Please try again.",
          thread_ts: thread,
        })
        return
      }

      log.info("Created opencode session:", createResult.data.id)

      session = { client, server, sessionId: createResult.data.id, channel, thread }
      sessions.set(sessionKey, session)

      const shareResult = await client.session.share({ path: { id: createResult.data.id } })
      if (!shareResult.error && shareResult.data) {
        const sessionUrl = shareResult.data.share?.url!
        log.debug("Session shared:", sessionUrl)
        await app.client.chat.postMessage({ channel, thread_ts: thread, text: sessionUrl })
      }
    }

    log.debug("Sending to opencode:", message.text)

    // Use prompt_async to avoid timeout - results come via event stream
    const result = await session.client.session.promptAsync({
      path: { id: session.sessionId },
      body: {
        agent: OPENCODE_AGENT,
        model: modelConfig,
        parts: [{ type: "text", text: message.text }]
      },
    })

    log.debug("Opencode prompt_async response:", JSON.stringify(result, null, 2))

    if (result.error) {
      log.error("Failed to send message:", result.error)
      await say({
        text: "Sorry, I had trouble processing your message. Please try again.",
        thread_ts: thread,
      })
      return
    }

    // Response will come via event stream, just acknowledge receipt
    log.debug("Message sent to opencode, waiting for response via events...")
  } catch (err) {
    log.error("Error processing message:", err)
    try {
      await say({
        text: "Sorry, an error occurred. Please try again.",
        thread_ts: (message as any).thread_ts || (message as any).ts,
      })
    } catch {}
  }
})

app.command("/test", async ({ command, ack, say }) => {
  await ack()
  log.debug("Test command received:", JSON.stringify(command, null, 2))
  await say("🤖 Bot is working! I can hear you loud and clear.")
})

await app.start()
log.info("Slack bot is running!")

// 이벤트 스트림 시작 (Slack 연결 후)
startEventStream().catch((err) => log.error("Event stream fatal error:", err))
