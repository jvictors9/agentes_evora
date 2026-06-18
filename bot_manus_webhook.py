"""
Bot do Telegram (BotFather) que conversa com o Manus — MODO WEBHOOK.

Fluxo:
  - Telegram envia cada mensagem para POST /telegram/webhook (confirmamos 200 na hora).
  - 1a mensagem do chat  -> cria tarefa no Manus (task.create)
    mensagens seguintes  -> continuam a mesma tarefa (task.sendMessage)
  - Quando o Manus conclui, ele chama POST /manus/webhook com o evento
    task_stopped, cujo campo task_detail.message traz a resposta. Entregamos
    ao chat certo usando o mapeamento task_id -> chat_id.

Requer um endpoint HTTPS público (PaaS, domínio próprio ou ngrok para testes).
Para um único usuário/equipe sem URL pública, use a versão por polling.
"""

import os
import time
import base64
import hashlib
import logging
from typing import Optional

import httpx
from fastapi import FastAPI, Request, Response, Header, BackgroundTasks
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.exceptions import InvalidSignature

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("manus-bot")

# --------------------------------------------------------------------------
# Configuração (variáveis de ambiente)
# --------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]            # do BotFather
TELEGRAM_SECRET_TOKEN = os.environ.get("TELEGRAM_SECRET_TOKEN", "")  # do setWebhook

MANUS_API_KEY = os.environ["MANUS_API_KEY"]
MANUS_BASE_URL = os.environ.get("MANUS_BASE_URL", "https://api.manus.ai/v2")
MANUS_AGENT_PROFILE = os.environ.get("MANUS_AGENT_PROFILE", "manus-1.6-lite")

# URL pública COMPLETA do endpoint /manus/webhook (precisa bater com a assinatura)
PUBLIC_MANUS_WEBHOOK_URL = os.environ.get("PUBLIC_MANUS_WEBHOOK_URL", "")

ALLOWED_CHAT_IDS = {
    int(x) for x in os.environ.get("ALLOWED_CHAT_IDS", "").replace(" ", "").split(",") if x
}

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
TELEGRAM_MAX = 4096

app = FastAPI(title="Manus Telegram Bot (webhook)")
client = httpx.AsyncClient(timeout=30)

# Estado em memória (uso interno). Em produção troque por Redis/SQLite.
chat_to_task: dict[int, str] = {}    # chat_id -> task_id (conversa contínua)
task_to_chat: dict[str, int] = {}    # task_id -> chat_id (roteia o resultado)
seen_updates: set[int] = set()       # dedupe de update_id do Telegram
_public_key_cache = {"pem": None, "fetched_at": 0.0}


# --------------------------------------------------------------------------
# Telegram
# --------------------------------------------------------------------------
async def tg_send(chat_id: int, text: str):
    if not text:
        return
    for i in range(0, len(text), TELEGRAM_MAX):
        try:
            r = await client.post(
                f"{TELEGRAM_API}/sendMessage",
                json={"chat_id": chat_id, "text": text[i:i + TELEGRAM_MAX]},
            )
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001
            logger.error("Falha ao enviar ao Telegram: %s", e)


async def tg_typing(chat_id: int):
    try:
        await client.post(
            f"{TELEGRAM_API}/sendChatAction",
            json={"chat_id": chat_id, "action": "typing"},
        )
    except Exception:  # noqa: BLE001
        pass


# --------------------------------------------------------------------------
# Manus
# --------------------------------------------------------------------------
def _manus_headers():
    return {"x-manus-api-key": MANUS_API_KEY, "Content-Type": "application/json"}


async def manus_create_task(prompt: str) -> Optional[str]:
    try:
        r = await client.post(
            f"{MANUS_BASE_URL}/task.create",
            headers=_manus_headers(),
            json={"message": {"content": prompt}, "agent_profile": MANUS_AGENT_PROFILE},
        )
        if r.status_code >= 400:
            # Mostra EXATAMENTE o que o Manus reclamou (motivo do 400/401/etc.)
            logger.error("Manus task.create %s -> resposta: %s", r.status_code, r.text)
            return None
        d = r.json()
        return d.get("task_id") or d.get("task_detail", {}).get("task_id")
    except Exception as e:  # noqa: BLE001
        logger.error("Erro ao criar tarefa no Manus: %s", e)
        return None


async def manus_send_message(task_id: str, prompt: str) -> bool:
    try:
        r = await client.post(
            f"{MANUS_BASE_URL}/task.sendMessage",
            headers=_manus_headers(),
            json={"task_id": task_id, "message": {"content": prompt}},
        )
        if r.status_code >= 400:
            logger.error("Manus task.sendMessage %s -> resposta: %s", r.status_code, r.text)
        return r.status_code < 300
    except Exception as e:  # noqa: BLE001
        logger.error("Erro ao continuar tarefa no Manus: %s", e)
        return False


def _bind(chat_id: int, task_id: str):
    chat_to_task[chat_id] = task_id
    task_to_chat[task_id] = chat_id


# --------------------------------------------------------------------------
# Lógica da conversa (roda em background; o webhook já confirmou 200)
# --------------------------------------------------------------------------
async def process_message(chat_id: int, text: str):
    if text.strip() == "/start":
        old = chat_to_task.pop(chat_id, None)
        if old:
            task_to_chat.pop(old, None)
        await tg_send(chat_id, "Olá! Sou a Sofia. Pode me mandar sua pergunta ou tarefa.")
        return

    await tg_typing(chat_id)

    task_id = chat_to_task.get(chat_id)
    if task_id is None:
        new_id = await manus_create_task(text)
        if not new_id:
            await tg_send(chat_id, "Não consegui iniciar agora. Tente novamente em instantes.")
            return
        _bind(chat_id, new_id)
    else:
        ok = await manus_send_message(task_id, text)
        if not ok:
            # a tarefa pode ter encerrado; começa uma nova
            new_id = await manus_create_task(text)
            if not new_id:
                await tg_send(chat_id, "Não consegui continuar agora. Tente novamente.")
                return
            _bind(chat_id, new_id)

    await tg_send(chat_id, "Recebi. Estou processando e já te respondo.")


# --------------------------------------------------------------------------
# Endpoint: webhook do Telegram
# --------------------------------------------------------------------------
@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    background: BackgroundTasks,
    x_telegram_bot_api_secret_token: str = Header(default=""),
):
    if TELEGRAM_SECRET_TOKEN and x_telegram_bot_api_secret_token != TELEGRAM_SECRET_TOKEN:
        return Response(status_code=403)

    update = await request.json()

    update_id = update.get("update_id")
    if update_id is not None:
        if update_id in seen_updates:
            return {"ok": True}
        seen_updates.add(update_id)

    msg = update.get("message") or {}
    text = msg.get("text")
    chat_id = (msg.get("chat") or {}).get("id")

    if text and chat_id is not None:
        if ALLOWED_CHAT_IDS and chat_id not in ALLOWED_CHAT_IDS:
            background.add_task(tg_send, chat_id, "Este bot é de uso restrito.")
        else:
            background.add_task(process_message, chat_id, text)

    return {"ok": True}


# --------------------------------------------------------------------------
# Endpoint: webhook do Manus (resultado da tarefa)
# --------------------------------------------------------------------------
async def get_manus_public_key() -> Optional[str]:
    now = time.time()
    if _public_key_cache["pem"] and now - _public_key_cache["fetched_at"] < 3600:
        return _public_key_cache["pem"]
    try:
        r = await client.get(
            f"{MANUS_BASE_URL}/webhook.publicKey",
            headers={"x-manus-api-key": MANUS_API_KEY},
        )
        r.raise_for_status()
        pem = r.json()["public_key"]
        _public_key_cache.update(pem=pem, fetched_at=now)
        return pem
    except Exception as e:  # noqa: BLE001
        logger.error("Não consegui obter a chave pública do Manus: %s", e)
        return None


def verify_manus_signature(pem: str, url: str, body: bytes, signature_b64: str, timestamp: str) -> bool:
    try:
        if abs(int(time.time()) - int(timestamp)) > 300:  # janela de 5 min (anti-replay)
            return False
        body_hash = hashlib.sha256(body).hexdigest()
        signed_content = f"{timestamp}.{url}.{body_hash}".encode()
        key = serialization.load_pem_public_key(pem.encode())
        key.verify(
            base64.b64decode(signature_b64),
            signed_content,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return True
    except (InvalidSignature, Exception):  # noqa: BLE001
        return False


async def handle_task_stopped(detail: dict):
    task_id = detail.get("task_id")
    chat_id = task_to_chat.get(task_id)
    if chat_id is None:
        logger.warning("task_stopped de tarefa desconhecida: %s", task_id)
        return

    await tg_send(chat_id, detail.get("message", "Tarefa concluída."))

    for att in detail.get("attachments") or []:
        name = att.get("file_name", "arquivo")
        link = att.get("url", "")
        await tg_send(chat_id, f"Arquivo: {name}\n{link}")

    # Mantemos o mapeamento para permitir continuar a conversa na mesma tarefa.
    # (stop_reason "finish" = concluiu; "ask" = aguardando o usuário)


@app.post("/manus/webhook")
async def manus_webhook(
    request: Request,
    background: BackgroundTasks,
    x_webhook_signature: str = Header(default=""),
    x_webhook_timestamp: str = Header(default=""),
):
    raw = await request.body()
    pem = await get_manus_public_key()
    url = PUBLIC_MANUS_WEBHOOK_URL or str(request.url)

    if not pem or not verify_manus_signature(pem, url, raw, x_webhook_signature, x_webhook_timestamp):
        return Response(status_code=401)

    data = await request.json()
    if data.get("event_type") == "task_stopped":
        background.add_task(handle_task_stopped, data.get("task_detail", {}))

    return {"ok": True}  # precisa responder 200 em até 10s


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.on_event("shutdown")
async def shutdown():
    await client.aclose()
