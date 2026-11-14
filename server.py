# encoding: utf-8
import atexit
import logging
import os

import socketio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi_utils.tasks import repeat_every
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import JSONResponse

from helper.QueueLogging import start_queue_logging
from htnd.HtndMultiClient import HtndMultiClient


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
# Records go through a bounded queue to a listener thread, so a stalled log sink cannot block the event loop.
log_sink = logging.StreamHandler()
log_sink.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
atexit.register(start_queue_logging(logging.getLogger(), log_sink))
logger = logging.getLogger(__name__)

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins=[])
socket_app = socketio.ASGIApp(sio)

app = FastAPI(
    title="Htnd REST-API server",
    description="This server is to communicate with hoosat network via REST-API",
    version=os.getenv("VERSION", "tbd"),
    contact={
        "name": "lAmeR1"
    },
    license_info={
        "name": "MIT LICENSE"
    }
)

app.add_middleware(GZipMiddleware, minimum_size=500)

app.mount("/ws", socket_app)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PingResponse(BaseModel):
    serverVersion: str = "0.12.2"
    isUtxoIndexed: bool = True
    isSynced: bool = True


@app.get("/ping",
         include_in_schema=False,
         response_model=PingResponse)
async def ping_server():
    """
    Ping Pong
    """
    try:
        info = await htnd_client.htnds[0].request("getInfoRequest")
        assert info["getInfoResponse"]["isSynced"] is True

        return {
            "server_version": info["getInfoResponse"]["serverVersion"],
            "is_utxo_indexed": info["getInfoResponse"]["isUtxoIndexed"],
            "is_synced": info["getInfoResponse"]["isSynced"]
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Htnd not connected.")


htnd_hosts = []

for i in range(100):
    try:
        htnd_hosts.append(os.environ[f"HTND_HOSTS_{i + 1}"].strip())
    except KeyError:
        break

if not htnd_hosts:
    raise Exception('Please set at least HTND_HOSTS_1 environment variable.')

htnd_client = HtndMultiClient(htnd_hosts)
logger.info("Configured htnd hosts: %s", htnd_hosts)


@app.exception_handler(Exception)
async def unicorn_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception while serving %s %s", request.method, request.url.path, exc_info=exc)
    await htnd_client.initialize_all()
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error"
                 # "traceback": f"{traceback.format_exception(exc)}"
                 },
    )


@app.on_event("startup")
@repeat_every(seconds=60)
async def periodical_blockdag():
    logger.debug("Refreshing htnd client state")
    await htnd_client.initialize_all()
