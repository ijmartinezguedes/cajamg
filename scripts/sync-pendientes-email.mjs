// scripts/sync-pendientes-email.mjs
// Corre en GitHub Actions (sin límite de 150s como Supabase Edge Functions).
//
// Diseño en dos fases:
//  FASE 1: una sola conexión IMAP hace un barrido liviano (solo metadata:
//          envelope + bodyStructure) de todos los mails nuevos, para
//          encontrar cuáles tienen adjuntos PDF/imagen candidatos a factura.
//          Esto ya probamos que es rápido y confiable con este servidor.
//  FASE 2: para cada mail candidato, se abre una conexión IMAP NUEVA y se
//          baja el mail completo (fetch con source:true + mailparser) —
//          el servidor de este hosting no soporta pedir partes sueltas de
//          un mail (bodyParts / download), así que hay que bajarlo entero.
//          Si un mail puntual se cuelga o falla, se lo salta y se sigue con
//          el resto usando una conexión limpia — un mail con problemas
//          nunca frena a los demás.

import { ImapFlow } from "imapflow";
import { simpleParser } from "mailparser";

const SB_URL = process.env.SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const IMAP_HOST = process.env.IMAP_HOST;
const IMAP_PORT = Number(process.env.IMAP_PORT || "993");
const IMAP_USER = process.env.IMAP_USER;
const IMAP_PASSWORD = process.env.IMAP_PASSWORD;

const MAX_ADJUNTO_BYTES = 15 * 1024 * 1024;
const MIN_IMAGEN_BYTES = 15 * 1024; // imágenes menores a esto casi siempre son logos de firma
const TIMEOUT_DESCARGA_MS = 25000; // por mail individual en la Fase 2

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

function esCandidatoValido(meta) {
  if (meta.size > MAX_ADJUNTO_BYTES) return false;
  if (meta.mime.startsWith("image/") && meta.size < MIN_IMAGEN_BYTES) return false;
  return true;
}

function conTimeout(promesa, ms, etiqueta) {
  return Promise.race([
    promesa,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`Timeout (${ms / 1000}s) en: ${etiqueta}`)), ms)
    ),
  ]);
}

async function conectar() {
  const client = new ImapFlow({
    host: IMAP_HOST,
    port: IMAP_PORT,
    secure: true,
    auth: { user: IMAP_USER, pass: IMAP_PASSWORD },
    logger: false,
  });
  await client.connect();
  return client;
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
  const lastUid = await getLastUid();

  // ── FASE 1: barrido liviano con una sola conexión ──────────────────────
  console.log(`Conectando a ${IMAP_HOST}:${IMAP_PORT} como ${IMAP_USER}...`);
  const client1 = await conectar();
  console.log("Conectado. Iniciando Fase 1 (barrido de metadata)...");

  let maxUidActual;
  const candidatosGlobal = [];
  let mensajesRevisados = 0;

  try {
    const lock = await client1.getMailboxLock("INBOX");
    try {
      const status = await client1.status("INBOX", { uidNext: true });
      maxUidActual = (status.uidNext || 1) - 1;

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

      for await (const msg of client1.fetch(`${lastUid + 1}:${maxUidActual}`, { uid: true, envelope: true, bodyStructure: true }, { uid: true })) {
        mensajesRevisados++;
        const candidatos = encontrarAdjuntos(msg.bodyStructure).filter(esCandidatoValido);
        if (candidatos.length > 0) {
          const fromAddr = msg.envelope?.from?.[0]?.address || "desconocido";
          const asunto = msg.envelope?.subject || "(sin asunto)";
          candidatosGlobal.push({ uid: msg.uid, asunto, fromAddr, candidatos });
          console.log(`  · UID ${msg.uid} — "${asunto}" de ${fromAddr} — ${candidatos.length} adjunto(s) candidato(s)`);
        }
      }
    } finally {
      try { lock.release(); } catch { /* noop */ }
    }
  } finally {
    try { await client1.logout(); } catch { /* noop */ }
  }

  console.log(`Fase 1 completa: ${mensajesRevisados} mails revisados, ${candidatosGlobal.length} con adjuntos candidatos.\n`);

  // ── FASE 2: un mail a la vez, conexión nueva para cada uno ─────────────
  let procesados = 0;
  let creados = 0;
  let mailsConError = 0;

  for (const cand of candidatosGlobal) {
    console.log(`Procesando UID ${cand.uid} — "${cand.asunto}" de ${cand.fromAddr}`);
    let client2 = null;
    try {
      client2 = await conectar();
      const lock2 = await client2.getMailboxLock("INBOX");
      try {
        const fetched = await conTimeout(
          client2.fetchOne(cand.uid, { uid: true, source: true }, { uid: true }),
          TIMEOUT_DESCARGA_MS,
          "descarga completa del mail"
        );
        if (!fetched || !fetched.source) throw new Error("No se obtuvo el mail completo");

        const parsed = await simpleParser(fetched.source);

        for (const meta of cand.candidatos) {
          const att = (parsed.attachments || []).find((a) => (a.filename || "") === meta.filename);
          if (!att) {
            console.log(`  ⚠️  No encontré "${meta.filename}" al parsear el mail — se salta`);
            continue;
          }
          procesados++;
          const base64 = att.content.toString("base64");
          const analysis = await analizarConIA(base64, meta.mime);

          if (analysis?.acreedor && analysis?.monto) {
            const ok = await crearPendiente({
              id: `pend_mail_${cand.uid}_${meta.part}_${Date.now()}`,
              acreedor: analysis.acreedor,
              moneda: analysis.moneda === "usd" ? "usd" : "peso",
              monto: analysis.monto,
              vto: analysis.vencimiento || null,
              pagado: false,
              cargado_por: "Automático (mail)",
              origen: "email",
              remitente: cand.fromAddr,
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
        }
      } finally {
        try { lock2.release(); } catch { /* noop */ }
      }
    } catch (err) {
      mailsConError++;
      console.error(`  ❌ No se pudo procesar este mail: ${err.message || err} — se salta y sigue con el resto`);
    } finally {
      try { await client2?.logout(); } catch { /* noop */ }
    }
  }

  // El barrido de la Fase 1 ya recorrió TODO el rango — guardamos como
  // procesado hasta ahí siempre, hayan fallado algunos mails puntuales o no.
  // Los que fallaron se pueden cargar a mano; reintentarlos por siempre
  // trabaría el avance para el resto.
  await setLastUid(maxUidActual);

  console.log(
    `\nListo. Mails revisados: ${mensajesRevisados}. Con adjuntos candidatos: ${candidatosGlobal.length}. ` +
    `Adjuntos procesados: ${procesados}. Pendientes creados: ${creados}. Mails con error: ${mailsConError}.`
  );
}

process.on("unhandledRejection", (reason) => {
  console.error("⚠️  unhandledRejection:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("⚠️  uncaughtException:", err);
});

main().catch((err) => {
  console.error("Error fatal:", err);
  process.exit(1);
});
