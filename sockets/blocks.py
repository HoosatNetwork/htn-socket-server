# encoding: utf-8

import asyncio
import logging
import time

from htnd.HtndThread import HtndThread
from server import htnd_client, sio
BLOCKS_CACHE = []
TASKS = []
# HTND disconnects an RPC client whose notification route backs up, so the stream reader only queues blocks.
BLOCK_QUEUE_SIZE = 1000
# Above this backlog blocks are emitted without getBlock hydration (txCount still comes from transactionIds).
HYDRATE_MAX_BACKLOG = 10
HYDRATE_TIMEOUT = 10  # seconds, one attempt; a failed hydration is emitted header-only
# Overflow drops are counted in the reader and reported at most this often, and whenever the queue drains.
DROP_REPORT_SECONDS = 1.0
logger = logging.getLogger(__name__)


async def config():
    logger.info("Starting block notification subscription")

    def render_transactions(transactions):
        rendered_transactions = []
        for transaction in transactions[-20:]:
            transaction_verbose_data = transaction.get("verboseData") or {}
            outputs = transaction.get("outputs") or []
            rendered_transactions.append({
                'txId': transaction_verbose_data.get("transactionId"),
                'outputs': [
                    ((output.get("verboseData") or {}).get("scriptPublicKeyAddress"), output.get("amount"))
                    for output in outputs[-20:]
                ]
            })

        return rendered_transactions

    block_queue = asyncio.Queue(maxsize=BLOCK_QUEUE_SIZE)
    drops = {"n": 0, "first": None, "last": None, "t": float("-inf")}
    in_flight = {"hash": None}

    def report_drops(force=False):
        if not drops["n"]:
            return
        now = time.monotonic()
        if not force and now - drops["t"] < DROP_REPORT_SECONDS:
            return
        logger.warning(
            "Block queue full (%s); dropped %s oldest blocks first_hash=%s last_hash=%s",
            BLOCK_QUEUE_SIZE, drops["n"], drops["first"], drops["last"],
        )
        drops.update(n=0, first=None, last=None, t=now)

    async def on_new_block(e):
        if "notifyBlockAddedResponse" in e:
            logger.info("Block notification subscription acknowledged")
            return

        block_added_notification = e.get("blockAddedNotification")
        if not block_added_notification:
            logger.warning("Received unexpected block notification payload: %s", e)
            return

        if block_queue.full():
            dropped = (block_queue.get_nowait().get("verboseData") or {}).get("hash")
            if not drops["n"]:
                drops["first"] = dropped
            drops["n"] += 1
            drops["last"] = dropped
            report_drops()
        block_queue.put_nowait(block_added_notification.get("block") or {})

    async def process_blocks(htnd):
        # one channel for all hydration requests of this subscription
        async with HtndThread(htnd.htnd_host, htnd.htnd_port) as hydrate_thread:
            while True:
                block_info = await block_queue.get()
                in_flight["hash"] = (block_info.get("verboseData") or {}).get("hash")
                try:
                    await process_block(block_info, hydrate_thread)
                except Exception:
                    logger.exception("Failed processing block notification")
                in_flight["hash"] = None  # stays set if cancelled mid-block: delivery unknown
                if block_queue.empty():
                    report_drops(force=True)  # drops after the last throttled report must not wait for another overflow

    async def process_block(block_info, hydrate_thread):
        header = block_info.get("header") or {}
        verbose_data = block_info.get("verboseData") or {}
        transactions = block_info.get("transactions") or []
        transaction_ids = verbose_data.get("transactionIds") or []
        block_hash = verbose_data.get("hash")

        if not transactions and transaction_ids and block_hash and block_queue.qsize() >= HYDRATE_MAX_BACKLOG:
            logger.info("Skipping hydration for block hash=%s; backlog=%s", block_hash, block_queue.qsize())
        elif not transactions and transaction_ids and block_hash:
            logger.info(
                "Hydrating transactions for block hash=%s using getBlockRequest",
                block_hash,
            )
            try:
                block_response = await hydrate_thread.request(
                    "getBlockRequest",
                    {
                        "hash": block_hash,
                        "includeTransactions": True,
                    },
                    wait_for_response=True,
                    timeout=HYDRATE_TIMEOUT,
                )
                transactions = ((block_response or {}).get("getBlockResponse") or {}).get("block", {}).get("transactions") or []
            except Exception:
                logger.exception("Failed hydrating transactions for block hash=%s", block_hash)

        tx_count = len(transactions) if transactions else len(transaction_ids)
        rendered_transactions = render_transactions(transactions)

        global BLOCKS_CACHE
        emit_info = {
            'block_hash': block_hash,
            'difficulty': verbose_data.get("difficulty"),
            'blueScore': header.get("blueScore", verbose_data.get("blueScore")),
            'timestamp': header.get("timestamp"),
            'txCount': tx_count,
            'txs': rendered_transactions,
        }

        logger.info(
            "Received new block hash=%s blueScore=%s txCount=%s headerOnly=%s",
            emit_info["block_hash"],
            emit_info["blueScore"],
            emit_info["txCount"],
            verbose_data.get("isHeaderOnly"),
        )
        logger.info(
            "Prepared block payload hash=%s rendered_txs=%s",
            emit_info["block_hash"],
            len(rendered_transactions),
        )

        BLOCKS_CACHE.append(emit_info)
        if len(BLOCKS_CACHE) > 10:
            BLOCKS_CACHE.pop(0)

        logger.debug("Block cache size is now %s", len(BLOCKS_CACHE))

        try:
            await sio.emit("new-block", emit_info, room="blocks")
        except Exception:
            logger.exception("Failed emitting new-block for hash=%s", emit_info["block_hash"])
            raise

        logger.info("Emitted new-block event for hash=%s to room=blocks", emit_info["block_hash"])

    # Hydration and the notification stream use the same node; on failure the watchdog reinitializes and restarts.
    htnd = await htnd_client._get_or_initialize_htnd()
    logger.info("Subscribing to notifyBlockAddedRequest on %s:%s", htnd.htnd_host, htnd.htnd_port)
    worker = asyncio.create_task(process_blocks(htnd))
    reader = asyncio.create_task(htnd.notify("notifyBlockAddedRequest", None, on_new_block))
    try:
        done, _ = await asyncio.wait({worker, reader}, return_when=asyncio.FIRST_COMPLETED)
        if worker in done:
            await worker  # surfaces the worker's failure to the watchdog
            raise RuntimeError("Block worker ended unexpectedly")
        await reader
    except Exception:
        logger.exception("Block notification subscription terminated unexpectedly")
        raise
    finally:
        tasks = (reader, worker)
        running = [task for task in tasks if not task.done()]
        for task in running:
            task.cancel()
        # joins both children, so each channel close was awaited; a failed close is logged, the primary failure stays the raised one
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for task, result in zip(tasks, results):
            if task in running and isinstance(result, Exception):
                logger.error("Block subscription cleanup failed", exc_info=result)
        report_drops(force=True)
        pending = []
        while not block_queue.empty():
            pending.append((block_queue.get_nowait().get("verboseData") or {}).get("hash"))
        if pending or in_flight["hash"]:
            logger.warning(
                "Block subscription ended: discarded_pending=%s first_hash=%s last_hash=%s in_flight_hash=%s (delivery unknown)",
                len(pending), pending[0] if pending else None, pending[-1] if pending else None, in_flight["hash"],
            )


@sio.on("last-blocks")
async def get_last_blocks(sid, msg):
    logger.info("Socket %s requested last-blocks; cache_size=%s", sid, len(BLOCKS_CACHE))
    await sio.emit("last-blocks", BLOCKS_CACHE, to=sid)
