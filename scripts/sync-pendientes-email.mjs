// scripts/sync-pendientes-email.mjs
// Corre en GitHub Actions (sin límite de 150s como Supabase Edge Functions).
// Lee mails nuevos del buzón IMAP, detecta adjuntos PDF/imagen sin bajar el
// mail completo, los analiza con la IA de Anthropic, y crea pendientes en
// Supabase. Usa el mismo marcador de UID que antes (tabla email_sync_state),
// así que es compatible con lo que ya está corrido.

import { ImapFlow } from "imapflow";
import { simpleParser } from "mailparser";

const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const IMAP_HOST = process.env.IMAP_HOST;
const IMAP_PORT = Number(process.env.IMAP_PORT || "993");
const IMAP_USER = process.env.IMAP_USER;
const IMAP_PASSWORD = process.env.IMAP_PASSWORD;

const MAX_ADJUNTO_BYTES = 15 * 1024 * 1024; // acá sí podemos ser generosos, no hay apuro de tiempo
const MIN_IMAGEN_BYTES = 15 * 1024; // imágenes menores a esto casi siempre son logos de firma, no facturas
const TIMEOUT_DESCARGA_MS = 20000; // si bajar un adjunto puntual se cuelga, lo salteamos

for (const [k, v] of Object.entries({ SB_URL, SB_KEY, ANTHROPIC_KEY, IMAP_HOST, IMAP_USER, IMAP_PASSWORD })) {
  if (!v) {
    console.error(`Falta la variable de entorno: ${k}`);
    process.exit(1);
  }
}

function sbHeaders(extra = {}) {
  return {
    apikey: SB_KEY,
    Authorization: `Bearer ${SB_KEY}`,
    "Content-Type": "application/json",
    ...extra,
  };
}

async function getLastUid() {
  const res = await fetch(`${SB_URL}/rest/v1/email_sync_state?id=eq.1&select=last_uid`, {
    headers: sbHeaders(),
  });
  const rows = await res.json().catch(() => []);
  if (Array.isArray(rows) && rows.length > 0) return Number(rows[0].last_uid) || 0;
  return 0;
}

async function setLastUid(uid) {
  await fetch(`${SB_URL}/rest/v1/email_sync_state`, {
    method: "POST",
    headers: sbHeaders({ Prefer: "resolution=merge-duplicates" }),
    body: JSON.stringify({ id: 1, last_uid: uid, updated_at: new Date().toISOString() }),
  });
}

function encontrarAdjuntos(node, acc = []) {
  if (!node) return acc;
  const mime = String(node.type || "").toLowerCase();
  const filename =
    node.dispositionParameters?.filename ||
    node.parameters?.name ||
    node.dispositionParameters?.name;
  const esAdjunto = node.disposition === "attachment" || !!filename;

  if ((mime === "application/pdf" || mime.startsWith("image/")) && esAdjunto) {
    acc.push({
      part: node.part,
      mime,
      filename: filename || `adjunto.${mime.split("/")[1] || "bin"}`,
      size: Number(node.size) || 0,
    });
  }
  if (Array.isArray(node.childNodes)) {
    for (const child of node.childNodes) encontrarAdjuntos(child, acc);
  }
  return acc;
}

async function streamToBase64(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks).toString("base64");
}

function conTimeout(promesa, ms, etiqueta) {
  return Promise.race([
    promesa,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`Timeout (${ms / 1000}s) en: ${etiqueta}`)), ms)
    ),
  ]);
}

async function analizarConIA(base64, mediaType) {
  const contentBlock = mediaType === "application/pdf"
    ? { type: "document", source: { type: "base64", media_type: "application/pdf", data: base64 } }
    : { type: "image", source: { type: "base64", media_type: mediaType, data: base64 } };

  const resp = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": ANTHROPIC_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: "claude-sonnet-4-6",
      max_tokens: 500,
      messages: [{
        role: "user",
        content: [
          contentBlock,
          {
            type: "text",
            text:
              `Analizá esta factura y extraé la información. Respondé SOLO con JSON válido, sin texto extra ni backticks:\n` +
              `{"acreedor":"nombre empresa/proveedor que emite la factura","monto":número_total,"moneda":"peso o usd","vencimiento":"YYYY-MM-DD o null","confianza":"alta|media|baja"}\n` +
              `Moneda: peso=UYU/$. usd=USD/U$S/dólares. Monto=importe total a pagar con IVA incluido.`,
          },
        ],
      }],
    }),
  });

  if (!resp.ok) {
    console.error("Anthropic API error:", resp.status, await resp.text().catch(() => ""));
    return null;
  }
  const data = await resp.json();
  const text = data.content?.find((b) => b.type === "text")?.text || "";
  const clean = text.replace(/```json|```/g, "").trim();
  try {
    return JSON.parse(clean);
  } catch {
    return null;
  }
}

async function crearPendiente(row) {
  const res = await fetch(`${SB_URL}/rest/v1/pendientes`, {
    method: "POST",
    headers: sbHeaders({ Prefer: "resolution=merge-duplicates" }),
    body: JSON.stringify(row),
  });
  if (!res.ok) {
    console.error("Error insertando pendiente:", res.status, await res.text().catch(() => ""));
  }
  return res.ok;
}

async function main() {
  console.log(`Conectando a ${IMAP_HOST}:${IMAP_PORT} como ${IMAP_USER}...`);
  const client = new ImapFlow({
    host: IMAP_HOST,
    port: IMAP_PORT,
    secure: true,
    auth: { user: IMAP_USER, pass: IMAP_PASSWORD },
    logger: false,
  });

  await client.connect();
  console.log("Conectado.");

  const lock = await client.getMailboxLock("INBOX");
  let creados = 0;
  let procesados = 0;

  try {
    const status = await client.status("INBOX", { uidNext: true });
    const maxUidActual = (status.uidNext || 1) - 1;
    const lastUid = await getLastUid();

    if (lastUid === 0) {
      await setLastUid(maxUidActual);
      console.log("Primera sincronización: se estableció el punto de partida, no se procesó historial.");
      return;
    }

    if (maxUidActual <= lastUid) {
      console.log("No hay mails nuevos.");
      return;
    }

    console.log(`Procesando UIDs ${lastUid + 1} a ${maxUidActual} (${maxUidActual - lastUid} mails)...`);

    let contador = 0;
    let conexionRota = false;
    for await (const msg of client.fetch(`${lastUid + 1}:${maxUidActual}`, { uid: true, envelope: true, bodyStructure: true }, { uid: true })) {
      if (conexionRota) break;
      contador++;
      const fromAddr = msg.envelope?.from?.[0]?.address || "desconocido";
      const asunto = msg.envelope?.subject || "(sin asunto)";
      const candidatos = encontrarAdjuntos(msg.bodyStructure).filter((meta) => {
        if (meta.size > MAX_ADJUNTO_BYTES) return false;
        if (meta.mime.startsWith("image/") && meta.size < MIN_IMAGEN_BYTES) return false;
        return true;
      });

      if (candidatos.length > 0) {
        console.log(`[${contador}] UID ${msg.uid} — "${asunto}" de ${fromAddr} — ${candidatos.length} adjunto(s) candidato(s)`);
        try {
          console.log(`  → bajando el mail completo (el servidor no soporta pedir partes sueltas)...`);
          const fetched = await conTimeout(
            client.fetchOne(msg.uid, { uid: true, source: true }, { uid: true }),
            TIMEOUT_DESCARGA_MS,
            `descarga completa del mail`
          );
          if (!fetched || !fetched.source) throw new Error("No se obtuvo el mail completo");
          const parsed = await simpleParser(fetched.source);
          console.log(`  → mail parseado, ${parsed.attachments?.length || 0} adjuntos reales encontrados`);

          for (const meta of candidatos) {
            const att = (parsed.attachments || []).find((a) => (a.filename || "") === meta.filename);
            if (!att) {
              console.log(`  ⚠️  No encontré "${meta.filename}" al parsear el mail — se salta`);
              continue;
            }
            procesados++;
            try {
              const base64 = att.content.toString("base64");
              console.log(`  → base64 listo para "${meta.filename}" (${base64.length} chars), llamando a la IA...`);
              const analysis = await analizarConIA(base64, meta.mime);
              console.log(`  → respuesta de la IA:`, JSON.stringify(analysis));

              if (analysis?.acreedor && analysis?.monto) {
                const ok = await crearPendiente({
                  id: `pend_mail_${msg.uid}_${meta.part}_${Date.now()}`,
                  acreedor: analysis.acreedor,
                  moneda: analysis.moneda === "usd" ? "usd" : "peso",
                  monto: analysis.monto,
                  vto: analysis.vencimiento || null,
                  pagado: false,
                  cargado_por: "Automático (mail)",
                  origen: "email",
                  remitente: fromAddr,
                  archivo_name: meta.filename,
                  archivo_type: meta.mime,
                  archivo_data: `data:${meta.mime};base64,${base64}`,
                });
                if (ok) {
                  creados++;
                  console.log(`  ✅ Pendiente creado: ${analysis.acreedor} — ${analysis.monto} (confianza: ${analysis.confianza})`);
                } else {
                  console.log(`  ❌ No se pudo guardar en Supabase`);
                }
              } else {
                console.log(`  ⚠️  La IA no pudo extraer los datos de "${meta.filename}"`);
              }
            } catch (err) {
              console.error(`  ❌ Error analizando "${meta.filename}":`, err.message || err);
            }
          }
        } catch (err) {
          console.error(`  ❌ Error bajando el mail completo:`, err.message || err);
          if (String(err.message || "").startsWith("Timeout")) {
            conexionRota = true;
            console.log(`  🛑 Se corta la corrida acá — la conexión puede haber quedado inestable. Lo que ya se guardó queda a salvo; la próxima corrida sigue desde el mail anterior a este.`);
          }
        }
      }

      // Guardamos progreso solo si este mail se terminó de revisar entero —
      // así, si se corta a mitad de camino, la próxima corrida retoma este
      // mismo mail desde cero en vez de saltearlo.
      if (!conexionRota) {
        await setLastUid(msg.uid);
      }
    }

    if (!conexionRota) {
      await setLastUid(maxUidActual);
    }
    console.log(`\nListo. Mails revisados: ${contador}. Adjuntos procesados: ${procesados}. Pendientes creados: ${creados}.${conexionRota ? " (corrida interrumpida por timeout — la próxima corrida sigue desde acá)" : ""}`);
  } finally {
    try { lock.release(); } catch { /* la conexión puede ya estar rota, no importa */ }
    try { await client.logout(); } catch { /* idem */ }
  }
}

process.on("unhandledRejection", (reason) => {
  console.error("⚠️  unhandledRejection (esto explicaría un corte silencioso):", reason);
  process.exitCode = 1;
});
process.on("uncaughtException", (err) => {
  console.error("⚠️  uncaughtException (esto explicaría un corte silencioso):", err);
  process.exitCode = 1;
});

main().catch((err) => {
  console.error("Error fatal:", err);
  process.exit(1);
});
