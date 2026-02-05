import { App } from "@slack/bolt"
import { createOpencode, type ToolPart } from "@opencode-ai/sdk"

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  signingSecret: process.env.SLACK_SIGNING_SECRET,
  socketMode: true,
  appToken: process.env.SLACK_APP_TOKEN,
})

// Agent configuration (default: router)
const OPENCODE_AGENT = process.env.OPENCODE_AGENT || "router"

console.log("🔧 Bot configuration:")
console.log("- Bot token present:", !!process.env.SLACK_BOT_TOKEN)
console.log("- Signing secret present:", !!process.env.SLACK_SIGNING_SECRET)
console.log("- App token present:", !!process.env.SLACK_APP_TOKEN)
console.log("- OpenCode agent:", OPENCODE_AGENT)

console.log("🚀 Starting opencode server...")
const opencode = await createOpencode({
  port: 0,
})
console.log("✅ Opencode server ready")

const sessions = new Map<string, { client: any; server: any; sessionId: string; channel: string; thread: string }>()

// Track which messages we've already sent responses for
const sentMessages = new Set<string>()

;(async () => {
  const events = await opencode.client.event.subscribe()
  for await (const event of events.stream) {
    // Handle tool updates
    if (event.type === "message.part.updated") {
      const part = event.properties.part

      for (const [sessionKey, session] of sessions.entries()) {
        if (session.sessionId === part.sessionID) {
          if (part.type === "tool") {
            handleToolUpdate(part, session.channel, session.thread)
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
        console.log("📨 Assistant message completed:", msg.id)
        sentMessages.add(msg.id) // Mark as sent early to prevent duplicates

        // Find the session
        for (const [sessionKey, session] of sessions.entries()) {
          if (session.sessionId === msg.sessionID) {
            console.log("🔍 Found session, fetching message parts...")

            // Fetch the full message with parts
            const messageResult = await opencode.client.session.message({
              path: { id: msg.sessionID, messageID: msg.id }
            })

            console.log("📦 Message result:", JSON.stringify(messageResult, null, 2).substring(0, 500))

            if (messageResult.error) {
              console.error("❌ Error fetching message:", messageResult.error)
              break
            }

            if (messageResult.data) {
              const data = messageResult.data as any
              const parts = data.parts || []
              console.log("📋 Parts count:", parts.length, "Types:", parts.map((p: any) => p.type))

              // Extract agent info from data.info
              const info = data.info || {}
              const agentName = info.agent || "unknown"
              const modelID = info.modelID || ""
              const providerID = info.providerID || ""

              console.log("🤖 Agent info:", { agentName, modelID, providerID })

              const textParts = parts.filter((p: any) => p.type === "text")
              const responseText = textParts.map((p: any) => p.text).join("\n")

              console.log("📝 Response text length:", responseText?.length, "Preview:", responseText?.substring(0, 100))

              if (responseText && responseText.trim()) {
                // Format message with agent info header
                const agentHeader = `🤖 *${agentName}* | ${providerID}/${modelID}\n───────────────────\n`
                const formattedText = agentHeader + responseText

                console.log("💬 Sending to Slack:", formattedText.substring(0, 150) + "...")
                await app.client.chat.postMessage({
                  channel: session.channel,
                  thread_ts: session.thread,
                  text: formattedText,
                  reply_broadcast: true, // Also show in channel
                }).catch((err) => console.error("❌ Failed to send:", err))
                console.log("✅ Sent to Slack")
              } else {
                console.log("⚠️ No text content to send")
              }
            } else {
              console.log("⚠️ No data in message result")
            }
            break
          }
        }
      }
    }
  }
})()

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
  console.log("📡 Raw Slack event:", JSON.stringify(context, null, 2))
  await next()
})

app.message(async ({ message, say }) => {
  console.log("📨 Received message event:", JSON.stringify(message, null, 2))

  if (message.subtype || !("text" in message) || !message.text) {
    console.log("⏭️ Skipping message - no text or has subtype")
    return
  }

  console.log("✅ Processing message:", message.text)

  const channel = message.channel
  const thread = (message as any).thread_ts || message.ts
  const sessionKey = `${channel}-${thread}`

  let session = sessions.get(sessionKey)

  if (!session) {
    console.log("🆕 Creating new opencode session...")
    const { client, server } = opencode

    const createResult = await client.session.create({
      body: { title: `Slack thread ${thread}` },
    })

    if (createResult.error) {
      console.error("❌ Failed to create session:", createResult.error)
      await say({
        text: "Sorry, I had trouble creating a session. Please try again.",
        thread_ts: thread,
      })
      return
    }

    console.log("✅ Created opencode session:", createResult.data.id)

    session = { client, server, sessionId: createResult.data.id, channel, thread }
    sessions.set(sessionKey, session)

    const shareResult = await client.session.share({ path: { id: createResult.data.id } })
    if (!shareResult.error && shareResult.data) {
      const sessionUrl = shareResult.data.share?.url!
      console.log("🔗 Session shared:", sessionUrl)
      await app.client.chat.postMessage({ channel, thread_ts: thread, text: sessionUrl })
    }
  }

  console.log("📝 Sending to opencode:", message.text)

  // Use prompt_async to avoid timeout - results come via event stream
  const result = await session.client.session.promptAsync({
    path: { id: session.sessionId },
    body: {
      agent: OPENCODE_AGENT,
      parts: [{ type: "text", text: message.text }]
    },
  })

  console.log("📤 Opencode prompt_async response:", JSON.stringify(result, null, 2))

  if (result.error) {
    console.error("❌ Failed to send message:", result.error)
    await say({
      text: "Sorry, I had trouble processing your message. Please try again.",
      thread_ts: thread,
    })
    return
  }

  // Response will come via event stream, just acknowledge receipt
  console.log("✅ Message sent to opencode, waiting for response via events...")
})

app.command("/test", async ({ command, ack, say }) => {
  await ack()
  console.log("🧪 Test command received:", JSON.stringify(command, null, 2))
  await say("🤖 Bot is working! I can hear you loud and clear.")
})

await app.start()
console.log("⚡️ Slack bot is running!")
