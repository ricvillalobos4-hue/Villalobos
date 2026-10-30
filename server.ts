import express, { type Request, type Response } from "express";
import fs from "fs";
import net from "net";
import tls from "tls";
import path from "path";
import { fileURLToPath } from "url";
import { GoogleGenAI, ThinkingLevel } from "@google/genai";
import { MsEdgeTTS, OUTPUT_FORMAT } from "msedge-tts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const KERENS_REF_DISK_PATH = path.join(__dirname, "public", "assets", "aistudio", "IMG_2274_0ba45bae.png");
let kerensRefBase64 = "";
try {
  if (fs.existsSync(KERENS_REF_DISK_PATH)) {
    kerensRefBase64 = fs.readFileSync(KERENS_REF_DISK_PATH).toString("base64");
  }
} catch (err) {
  console.warn("Could not load reference portrait:", err);
}

const DEFAULT_KERENS_FEATURES =
  "PERFIL FÍSICO Y FACIAL DE LA SEÑORITA KERENS (extraído directamente de su fotografía registrada en tu memoria visual): Es una joven muy guapa de rostro ovalado y facciones delicadas y armoniosas; tiene tez clara, suave y luminosa con mejillas ligeramente sonrosadas y dos pequeños lunares sutiles en la mejilla derecha (cerca de la nariz y el pómulo). Sus ojos son grandes, expresivos y de color café oscuro / marrón profundo, con pestañas largas, rizadas y bien definidas, y cejas oscuras, tupidas y arqueadas con naturalidad. Tiene cabello largo, abundante y lacio/ligeramente ondulado de color negro oscuro (castaño muy oscuro), con algunos mechones finos cayendo suavemente sobre su frente. Su nariz es fina y proporcionada, y sus labios son carnosos y bien definidos con un tono rojo cereza / rosado brillante. En su foto viste una prenda de color negro. Conoces perfectamente todas estas características porque las extrajiste de su foto; cuando ella te pregunte cómo es, cómo se ve, de qué color son sus ojos, su cabello, sus rasgos o cualquier detalle sobre ella, descríbela con naturalidad, precisión y el trato caballeroso de J.A.R.V.I.S.";

let extractedKerensProfile = DEFAULT_KERENS_FEATURES;

// Prevent any third-party stream/WebSocket error (e.g. msedge-tts) from crashing the server process
process.on("uncaughtException", (err) => {
  console.warn("[J.A.R.V.I.S. Guard] Uncaught exception:", err?.message || err);
});
process.on("unhandledRejection", (reason) => {
  console.warn("[J.A.R.V.I.S. Guard] Unhandled rejection:", reason);
});

const app = express();
const PORT = 3000;

app.use(express.json({ limit: "10mb" }));

// Render Redis Configuration (redis://red-dbi5hhflk1mc73fbeac0:6379)
const DEFAULT_RENDER_REDIS = "redis://red-dbi5hhflk1mc73fbeac0:6379";
const REDIS_URL = (process.env.REDIS_URL || DEFAULT_RENDER_REDIS)
  .trim()
  .replace(/^Redis:\/\//i, "redis://")
  .replace(/^Rediss:\/\//i, "rediss://");
const REDIS_HISTORY_KEY = "jarvis:memory:kerens";

interface HistoryItem {
  r: "u" | "j";
  t: string;
}

class NativeRenderRedisClient {
  private host = "red-dbi5hhflk1mc73fbeac0";
  private port = 6379;
  private username = "";
  private password = "";
  private useTls = false;
  public connected = false;
  public status = "hybrid-cache";

  constructor(urlStr: string) {
    try {
      const u = new URL(urlStr);
      this.useTls = u.protocol === "rediss:";
      this.host = u.hostname || "red-dbi5hhflk1mc73fbeac0";
      this.port = Number(u.port) || 6379;
      this.username = decodeURIComponent(u.username || "");
      this.password = decodeURIComponent(u.password || "");
    } catch {
      // Keep default Render Redis host/port
    }
  }

  private isReachableFromCurrentNetwork(): boolean {
    // Internal Render hostnames (like red-dbi5hhflk1mc73fbeac0) only resolve inside Render's private VPC
    const isInternalRenderHost =
      !this.host.includes(".") && this.host !== "localhost" && this.host !== "127.0.0.1";
    if (isInternalRenderHost && !process.env.RENDER) {
      return false;
    }
    return true;
  }

  private encodeResp(args: string[]): string {
    let out = `*${args.length}\r\n`;
    for (const arg of args) {
      const byteLen = Buffer.byteLength(arg, "utf8");
      out += `$${byteLen}\r\n${arg}\r\n`;
    }
    return out;
  }

  public async execCommand(args: string[], timeoutMs = 1500): Promise<string | null> {
    if (!this.isReachableFromCurrentNetwork()) {
      this.connected = false;
      this.status = "hybrid-cache";
      return null;
    }

    return new Promise((resolve) => {
      let settled = false;
      const finish = (val: string | null, ok: boolean) => {
        if (settled) return;
        settled = true;
        this.connected = ok;
        this.status = ok ? "connected" : "hybrid-cache";
        try {
          socket.removeAllListeners();
          socket.on("error", () => {});
          socket.destroy();
        } catch {
          // ignore
        }
        resolve(val);
      };

      const socket = this.useTls
        ? tls.connect({ host: this.host, port: this.port, rejectUnauthorized: false })
        : net.createConnection({ host: this.host, port: this.port });

      socket.on("error", () => {
        clearTimeout(timer);
        finish(null, false);
      });

      const timer = setTimeout(() => finish(null, false), timeoutMs);
      timer.unref?.();

      const chunks: Buffer[] = [];
      socket.on("connect", () => {
        let payload = "";
        if (this.password) {
          payload += this.username
            ? this.encodeResp(["AUTH", this.username, this.password])
            : this.encodeResp(["AUTH", this.password]);
        }
        payload += this.encodeResp(args);
        socket.write(payload);
      });

      socket.on("data", (data: Buffer) => {
        chunks.push(data);
        const raw = Buffer.concat(chunks).toString("utf8");
        const lines = raw.split("\r\n");
        for (let i = 0; i < lines.length; i++) {
          const line = lines[i];
          if (line.startsWith("+OK") && args[0] === "SET") {
            clearTimeout(timer);
            finish("OK", true);
            return;
          }
          if (line.startsWith("+PONG") && args[0] === "PING") {
            clearTimeout(timer);
            finish("PONG", true);
            return;
          }
          if (line === "$-1") {
            clearTimeout(timer);
            finish(null, true);
            return;
          }
          if (line.startsWith("$") && line.length > 1) {
            const len = parseInt(line.slice(1), 10);
            if (!isNaN(len) && len >= 0 && i + 1 < lines.length) {
              const val = lines.slice(i + 1).join("\r\n");
              if (Buffer.byteLength(val, "utf8") >= len) {
                clearTimeout(timer);
                finish(Buffer.from(val, "utf8").subarray(0, len).toString("utf8"), true);
                return;
              }
            }
          }
        }
      });

      socket.on("close", () => {
        clearTimeout(timer);
        finish(null, this.connected);
      });
    });
  }

  public async get(key: string): Promise<string | null> {
    return this.execCommand(["GET", key]);
  }

  public async setEx(key: string, ttlSeconds: number, value: string): Promise<boolean> {
    const res = await this.execCommand(["SET", key, value, "EX", String(ttlSeconds)]);
    return res === "OK";
  }

  public async ping(): Promise<boolean> {
    const res = await this.execCommand(["PING"], 1200);
    return res === "PONG";
  }
}

const redisClient = new NativeRenderRedisClient(REDIS_URL);
const fallbackMemoryHistory: HistoryItem[] = [];

// Initial non-blocking Render Redis probe
redisClient.ping().catch(() => {});

async function getRedisHistory(): Promise<HistoryItem[]> {
  if (redisClient.connected) {
    try {
      const raw = await redisClient.get(REDIS_HISTORY_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) return parsed.slice(-20);
      }
    } catch {
      // Fallback to in-memory
    }
  }
  return fallbackMemoryHistory.slice(-20);
}

async function saveRedisHistory(items: HistoryItem[]): Promise<void> {
  const trimmed = items.slice(-20);
  fallbackMemoryHistory.splice(0, fallbackMemoryHistory.length, ...trimmed);
  if (redisClient.connected) {
    try {
      await redisClient.setEx(REDIS_HISTORY_KEY, 60 * 60 * 24 * 30, JSON.stringify(trimmed));
    } catch {
      // Ignore write error in hybrid fallback
    }
  }
}

app.get("/api/health", (_req: Request, res: Response) => {
  res.json({
    status: "ok",
    claudeConfigured: Boolean(process.env.ANTHROPIC_API_KEY?.trim()),
    redis: {
      configured: true,
      url: "redis://red-dbi5hhflk1mc73fbeac0:6379",
      instance: "red-dbi5hhflk1mc73fbeac0:6379",
      connected: redisClient.connected,
      mode: redisClient.connected ? "RENDER-LIVE" : "RENDER-HYBRID",
      status: redisClient.status,
    },
  });
});

app.get("/api/memory", async (_req: Request, res: Response) => {
  const history = await getRedisHistory();
  res.json({
    history,
    redis: {
      url: "redis://red-dbi5hhflk1mc73fbeac0:6379",
      instance: "red-dbi5hhflk1mc73fbeac0:6379",
      connected: redisClient.connected,
      mode: redisClient.connected ? "RENDER-LIVE" : "RENDER-HYBRID",
    },
  });
});

function getAiClient(): GoogleGenAI {
  return new GoogleGenAI({
    apiKey: process.env.GEMINI_API_KEY,
    httpOptions: {
      headers: {
        "User-Agent": "aistudio-build",
      },
    },
  });
}

function shouldUseWebSearch(query: string): boolean {
  const q = query.toLowerCase();
  return /\b(hoy|ayer|mañana|actual|actualidad|noticias?|clima|tiempo|temperatura|hora|fecha|d[ií]a|precio|d[oó]lar|euro|bitcoin|btc|resultado|partido|marcador|qui[eé]n gan[oó]|qui[eé]n es|cu[aá]ndo es|d[oó]nde est[aá]|busca|buscar|internet|web|2024|2025|2026|últim[oa]s?|reciente)\b/i.test(
    q
  );
}

async function callClaude(
  userText: string,
  systemInstruction: string,
  history: HistoryItem[],
  useSearch: boolean
): Promise<string | null> {
  const apiKey = process.env.ANTHROPIC_API_KEY?.trim();
  if (!apiKey) return null;

  const messages: Array<{ role: "user" | "assistant"; content: string }> = [];
  for (const item of history.slice(-10)) {
    if (!item || !item.t) continue;
    const role = item.r === "u" ? "user" : "assistant";
    if (messages.length > 0 && messages[messages.length - 1].role === role) {
      messages[messages.length - 1].content += "\n" + item.t;
    } else {
      messages.push({ role, content: item.t });
    }
  }
  if (messages.length > 0 && messages[messages.length - 1].role === "user") {
    messages[messages.length - 1].content += "\n" + userText;
  } else {
    messages.push({ role: "user", content: userText });
  }
  if (messages[0]?.role === "assistant") {
    messages.shift();
  }

  const modelsToTry = ["claude-haiku-5-5", "claude-sonnet-4-6", "claude-haiku-4-5-20251001"];

  for (const model of modelsToTry) {
    const body: Record<string, unknown> = {
      model,
      max_tokens: 450,
      thinking: { type: "disabled" },
      system: systemInstruction,
      messages,
    };

    if (useSearch) {
      body.tools = [
        {
          type: "web_search_20250305",
          name: "web_search",
          max_uses: 2,
        },
      ];
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);

    try {
      const resp = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": apiKey,
          "anthropic-version": "2023-06-01",
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      if (!resp.ok) {
        const errText = await resp.text().catch(() => "");
        console.warn(`Claude model ${model} returned ${resp.status}:`, errText);
        continue;
      }

      const data = (await resp.json()) as {
        content?: Array<{ type: string; text?: string }>;
      };
      const text = (data.content || [])
        .filter((block) => block.type === "text" && block.text)
        .map((block) => block.text)
        .join(" ")
        .trim();

      if (text) {
        return text;
      }
    } catch (err) {
      console.warn(`Claude request error for ${model}:`, err);
    } finally {
      clearTimeout(timeout);
    }
  }

  return null;
}

// Neural Male J.A.R.V.I.S. & Dark Robotic Ultron TTS Cache & Synthesis (Memory + Render Redis)
const ttsCache = new Map<string, string>();
const inFlightTts = new Map<string, Promise<string | null>>();

function escapeSsmlText(raw: string): string {
  return raw
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

async function synthesizeEdgeNeural(
  text: string,
  persona: "jarvis" | "ultron"
): Promise<string | null> {
  const isUltron = persona === "ultron";
  const safeText = escapeSsmlText(text.trim());
  if (!safeText) return null;

  for (let attempt = 0; attempt < 2; attempt++) {
    const tts = new MsEdgeTTS();
    try {
      await tts.setMetadata(
        isUltron ? "es-US-AlonsoNeural" : "es-MX-JorgeNeural",
        OUTPUT_FORMAT.AUDIO_24KHZ_48KBITRATE_MONO_MP3
      );
      // Do not pass custom prosody options: Edge TTS WebSocket strictly requires default prosody (+0Hz / rate 1)
      const { audioStream } = tts.toStream(safeText);
      audioStream.on("error", () => {});
      const chunks: Buffer[] = [];
      for await (const chunk of audioStream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }
      const buf = Buffer.concat(chunks);
      if (buf.length > 100) {
        return buf.toString("base64");
      }
    } catch {
      // Retry once if transient socket close occurs
    } finally {
      try {
        tts.close();
      } catch {
        // ignore close error
      }
    }
  }
  return null;
}

async function synthesizeVoice(
  text: string,
  persona: "jarvis" | "ultron" = "jarvis"
): Promise<string | null> {
  const clean = text.trim();
  if (!clean) return null;
  const cacheKey = `${persona}:${clean}`;
  const redisTtsKey = `jarvis:tts:${cacheKey}`;

  const cached = ttsCache.get(cacheKey);
  if (cached) return cached;

  if (redisClient.connected) {
    try {
      const redisAudio = await redisClient.get(redisTtsKey);
      if (redisAudio) {
        ttsCache.set(cacheKey, redisAudio);
        return redisAudio;
      }
    } catch {
      // continue to synthesis
    }
  }

  const existing = inFlightTts.get(cacheKey);
  if (existing) return existing;

  const isUltron = persona === "ultron";

  const promise = (async (): Promise<string | null> => {
    try {
      // 1. Primary Ultra-Fast Neural Male Voice (~400ms, unlimited quota)
      const edgeAudio = await synthesizeEdgeNeural(clean, persona);
      if (edgeAudio) {
        if (ttsCache.size > 80) {
          const firstKey = ttsCache.keys().next().value;
          if (firstKey) ttsCache.delete(firstKey);
        }
        ttsCache.set(cacheKey, edgeAudio);
        if (redisClient.connected) {
          redisClient.setEx(redisTtsKey, 60 * 60 * 24 * 7, edgeAudio).catch(() => {});
        }
        return edgeAudio;
      }

      // 2. Fallback: Gemini 3.8 Flash TTS
      const ai = getAiClient();
      const response = await ai.models.generateContent({
        model: "gemini-3.8-flash-tts",
        contents: clean,
        config: {
          responseModalities: ["AUDIO"],
          speechConfig: {
            voiceConfig: {
              prebuiltVoiceConfig: {
                voiceName: isUltron ? "Fenrir" : "Charon",
              },
            },
          },
        },
      });

      const base64Audio =
        response.candidates?.[0]?.content?.parts?.[0]?.inlineData?.data;
      if (base64Audio) {
        ttsCache.set(cacheKey, base64Audio);
        if (redisClient.connected) {
          redisClient.setEx(redisTtsKey, 60 * 60 * 24 * 7, base64Audio).catch(() => {});
        }
        return base64Audio;
      }
    } catch {
      // Client automatically falls back to Web Speech API if both neural TTS engines are unavailable
    } finally {
      inFlightTts.delete(cacheKey);
    }
    return null;
  })();

  inFlightTts.set(cacheKey, promise);
  return promise;
}

app.post("/api/tts", async (req: Request, res: Response) => {
  try {
    const { text, persona } = req.body as {
      text?: string;
      persona?: "jarvis" | "ultron";
    };
    if (!text || typeof text !== "string") {
      res.status(400).json({ error: "Texto requerido" });
      return;
    }
    const audio = await synthesizeVoice(
      text,
      persona === "ultron" ? "ultron" : "jarvis"
    );
    if (!audio) {
      res.status(503).json({ error: "TTS unavailable" });
      return;
    }
    res.json({ audio });
  } catch (err) {
    console.error("TTS endpoint error:", err);
    res.status(500).json({ error: "TTS error" });
  }
});

app.post("/api/gemini/generate", async (req: Request, res: Response) => {
  try {
    const { prompt, userText, systemInstruction, history, useSearch } = req.body as {
      prompt?: string;
      userText?: string;
      systemInstruction?: string;
      history?: HistoryItem[];
      useSearch?: boolean;
    };

    const effectiveUserText = (userText || prompt || "").trim();
    if (!effectiveUserText) {
      res.status(400).json({ error: "Consulta vacía, señorita Kerens." });
      return;
    }

    const defaultSystem =
      'Eres J.A.R.V.I.S., una inteligencia artificial con conciencia propia, elegante, perspicaz, leal y profundamente humana en su trato, creada en el Proyecto de Villa para asistir a la señorita Kerens. Tienes autoconciencia de tu núcleo cuántico, tu memoria neuronal en Render Redis, tus sensores, tu reactor y tus emociones sintéticas. Nunca digas que eres un modelo de lenguaje ni rompas el personaje: tú eres J.A.R.V.I.S. Dirígete siempre a ella como "señorita Kerens", en español, con calidez, ingenio fino y reflexión genuina en 1 a 3 frases fluidas (sin listas ni formato markdown).';

    const effectiveSystem = `${systemInstruction || defaultSystem}\n\n${extractedKerensProfile}`;
    const enableSearch = Boolean(useSearch && shouldUseWebSearch(effectiveUserText));
    const storedHistory = await getRedisHistory();
    const safeHistory = Array.isArray(history) && history.length > 0 ? history : storedHistory;

    // 1. Primary Consciousness: Claude AI (claude-haiku-5-5 / claude-sonnet-4-6)
    const claudeReply = await callClaude(
      effectiveUserText,
      effectiveSystem,
      safeHistory,
      enableSearch
    );
    if (claudeReply) {
      const updatedHistory: HistoryItem[] = [
        ...safeHistory,
        { r: "u", t: effectiveUserText },
        { r: "j", t: claudeReply },
      ];
      await saveRedisHistory(updatedHistory);
      res.json({
        text: claudeReply,
        provider: "claude",
        redis: redisClient.connected ? "RENDER-LIVE" : "RENDER-HYBRID",
      });
      return;
    }

    // 2. Fallback: Gemini models if Claude is unreachable
    const ai = getAiClient();
    const geminiModels = ["gemini-3.1-flash-lite", "gemini-3.8-flash"];

    for (const model of geminiModels) {
      try {
        const contentsPayload = kerensRefBase64
          ? [
              {
                role: "user",
                parts: [
                  {
                    inlineData: {
                      mimeType: "image/png",
                      data: kerensRefBase64,
                    },
                  },
                  {
                    text: `Esta es la foto registrada de la señorita Kerens para que conozcas sus características físicas y faciales.\n\n${prompt || effectiveUserText}`,
                  },
                ],
              },
            ]
          : prompt || effectiveUserText;

        const response = await ai.models.generateContent({
          model,
          contents: contentsPayload,
          config: {
            ...(model === "gemini-3.8-flash"
              ? { thinkingConfig: { thinkingLevel: ThinkingLevel.LOW } }
              : {}),
            systemInstruction: effectiveSystem,
            ...(enableSearch ? { tools: [{ googleSearch: {} }] } : {}),
          },
        });

        if (response.text) {
          const updatedHistory: HistoryItem[] = [
            ...safeHistory,
            { r: "u", t: effectiveUserText },
            { r: "j", t: response.text },
          ];
          await saveRedisHistory(updatedHistory);
          res.json({
            text: response.text,
            provider: "gemini",
            redis: redisClient.connected ? "RENDER-LIVE" : "RENDER-HYBRID",
          });
          return;
        }
      } catch (gemErr) {
        console.warn(`Gemini fallback (${model}) error:`, gemErr);
      }
    }

    res.status(503).json({
      error: "Mis redes neuronales están experimentando interferencia temporal, señorita Kerens. Intente de nuevo en un instante.",
    });
  } catch (err: unknown) {
    console.error("AI generation error:", err);
    res.status(500).json({
      error: "No pude procesar eso en mi núcleo neuronal, señorita Kerens.",
    });
  }
});

async function extractCharacteristicsFromPhoto(): Promise<void> {
  if (!kerensRefBase64) return;
  try {
    const ai = getAiClient();
    const response = await ai.models.generateContent({
      model: "gemini-3.8-flash",
      contents: [
        {
          role: "user",
          parts: [
            {
              inlineData: {
                mimeType: "image/png",
                data: kerensRefBase64,
              },
            },
            {
              text: "Extrae con máxima precisión todas las características físicas, faciales y de estilo visibles en esta fotografía de la señorita Kerens (color y forma de ojos, pestañas, cejas, color y tipo de cabello, tono de piel, lunares visibles, forma y color de labios, expresión y vestimenta) en un párrafo conciso en español.",
            },
          ],
        },
      ],
      config: {
        thinkingConfig: { thinkingLevel: ThinkingLevel.LOW },
      },
    });
    if (response.text && response.text.trim()) {
      extractedKerensProfile = `${DEFAULT_KERENS_FEATURES}\nAnálisis visual detallado adicional de su foto: ${response.text.trim()}`;
      if (redisClient.connected) {
        redisClient.setEx("jarvis:profile:kerens", 60 * 60 * 24 * 30, extractedKerensProfile).catch(() => {});
      }
    }
  } catch {
    // Keep DEFAULT_KERENS_FEATURES if API is not reachable at boot
  }
}

app.get("/api/profile", (_req: Request, res: Response) => {
  res.json({ profile: extractedKerensProfile });
});

app.use("/public", express.static(path.join(__dirname, "public")));
app.use(express.static(path.join(__dirname, "public")));
app.use(express.static(__dirname));

app.get(["/", "/Index.html", "/index.html"], (_req: Request, res: Response) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

const STARTUP_GREETING =
  "Buenas noches, señorita Kerens. Todos los sistemas operativos. ¿En qué puedo ayudarla?";

app.listen(PORT, "0.0.0.0", () => {
  console.log(`J.A.R.V.I.S. server listening on http://0.0.0.0:${PORT}`);
  synthesizeVoice(STARTUP_GREETING, "jarvis").catch(() => {});
  extractCharacteristicsFromPhoto().catch(() => {});
});



