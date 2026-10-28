// Zan AI — прокси к Google Gemini API.
// Ключ берётся из переменной окружения Netlify GEMINI_API_KEY (получить: aistudio.google.com → Get API key).
// Модель можно поменять переменной GEMINI_MODEL (по умолчанию — актуальная Flash-модель).
const MODEL = process.env.GEMINI_MODEL || "gemini-flash-latest";
const MAX_CHARS = 30000;

export default async (req) => {
  const key = process.env.GEMINI_API_KEY;
  if (req.method === "GET") return Response.json({ ok: Boolean(key) });
  if (req.method !== "POST") return new Response("Method not allowed", { status: 405 });
  if (!key) return Response.json({ error: { message: "GEMINI_API_KEY is not set" } }, { status: 500 });

  let body;
  try { body = await req.json(); } catch { return Response.json({ error: { message: "bad json" } }, { status: 400 }); }
  const raw = Array.isArray(body.messages) ? body.messages.slice(-10) : [];

  // Gemini: роли "user" и "model"; подряд идущие сообщения одной роли склеиваем
  const contents = [];
  let total = 0;
  for (const m of raw) {
    const role = m && m.role === "assistant" ? "model" : "user";
    const text = String((m && m.content) || "").slice(0, MAX_CHARS);
    total += text.length;
    const last = contents[contents.length - 1];
    if (last && last.role === role) last.parts[0].text += "\n\n" + text;
    else contents.push({ role, parts: [{ text }] });
  }
  if (!contents.length || contents[0].role !== "user" || total > MAX_CHARS * 2)
    return Response.json({ error: { message: "invalid messages" } }, { status: 400 });

  let upstream;
  try {
    upstream = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(MODEL)}:streamGenerateContent?alt=sse`,
      {
        method: "POST",
        headers: { "content-type": "application/json", "x-goog-api-key": key },
        body: JSON.stringify({ contents, generationConfig: { maxOutputTokens: 8192, temperature: 0.4 } }),
      }
    );
  } catch (e) {
    return Response.json({ error: { message: "Cannot reach Gemini API: " + e.message } }, { status: 502 });
  }

  if (!upstream.ok) {
    const text = await upstream.text();
    return new Response(text, { status: upstream.status, headers: { "content-type": "application/json" } });
  }

  // Переводим поток Gemini в простой формат, который понимает сайт
  const enc = new TextEncoder(), dec = new TextDecoder();
  const send = (obj) => enc.encode("data: " + JSON.stringify(obj) + "\n\n");
  const stream = new ReadableStream({
    async start(controller) {
      const reader = upstream.body.getReader();
      let buf = "";
      const handle = (line) => {
        if (!line.startsWith("data:")) return;
        let ev; try { ev = JSON.parse(line.slice(5)); } catch { return; }
        if (ev.error) { controller.enqueue(send({ type: "error", error: { type: "api_error", message: ev.error.message || "Gemini error" } })); return; }
        const parts = (ev.candidates && ev.candidates[0] && ev.candidates[0].content && ev.candidates[0].content.parts) || [];
        const text = parts.filter(p => p.text && !p.thought).map(p => p.text).join("");
        if (text) controller.enqueue(send({ type: "content_block_delta", delta: { type: "text_delta", text } }));
        const blocked = ev.promptFeedback && ev.promptFeedback.blockReason;
        if (blocked) controller.enqueue(send({ type: "error", error: { type: "api_error", message: "Blocked: " + blocked } }));
      };
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          let i;
          while ((i = buf.search(/\r?\n/)) >= 0) {
            const line = buf.slice(0, i); buf = buf.slice(buf[i] === "\r" ? i + 2 : i + 1);
            handle(line.trim());
          }
        }
        if (buf.trim()) handle(buf.trim());
      } catch (e) {
        controller.enqueue(send({ type: "error", error: { type: "api_error", message: e.message } }));
      }
      controller.close();
    },
  });
  return new Response(stream, { headers: { "content-type": "text/event-stream", "cache-control": "no-cache" } });
};

export const config = { path: "/api/ask" };
