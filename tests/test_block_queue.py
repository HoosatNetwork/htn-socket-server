# encoding: utf-8
# Self-contained check (no node, no grpc): python3 -B -m unittest tests/test_block_queue.py
import asyncio
import importlib.util
import logging
import os
import queue
import re
import sys
import threading
import time
import types
import unittest

HYDRATE_DELAY = 0.05
BURST = 1500


class FakeHtndThread(object):
    instances = []
    fail_on_enter = False
    close_delay = 0.05
    close_error = None  # raised by __aexit__ after the delay
    gate = started = None  # asyncio events: request() announces itself, then waits for the gate

    def __init__(self, host, port):
        self.host = host
        self.hydrated = []
        self.closed = False
        FakeHtndThread.instances.append(self)

    async def __aenter__(self):
        if FakeHtndThread.fail_on_enter:
            raise OSError("cannot create channel")
        return self

    async def __aexit__(self, *exc):
        await asyncio.sleep(FakeHtndThread.close_delay)  # a slow channel close
        self.closed = True
        if FakeHtndThread.close_error:
            raise FakeHtndThread.close_error

    async def request(self, command, params=None, wait_for_response=False, timeout=10):
        assert command == "getBlockRequest" and wait_for_response and timeout == blocks.HYDRATE_TIMEOUT
        self.hydrated.append(params["hash"])
        if FakeHtndThread.gate:
            FakeHtndThread.started.set()
            await FakeHtndThread.gate.wait()
        await asyncio.sleep(HYDRATE_DELAY)  # a slow node
        return {"getBlockResponse": {"block": {"transactions": [{"verboseData": {"transactionId": "tx"}}]}}}


class FakeSio(object):
    emit_delay = 0

    def __init__(self):
        self.emitted = []

    def on(self, _event):
        return lambda f: f

    async def emit(self, event, data, room=None, to=None):
        if self.emit_delay:
            await asyncio.sleep(self.emit_delay)
        self.emitted.append(data)


class FakeHtnd(object):
    htnd_host, htnd_port = "fake", "0"

    def __init__(self, script):
        self.script = script
        self.callback_times = []
        self.cancelled = False
        self.cancel_error = None  # raised instead of CancelledError, like a failing reader channel close

    async def notify(self, command, params, callback):
        try:
            await self.script(self, callback)
        except asyncio.CancelledError:
            self.cancelled = True
            if self.cancel_error:
                raise self.cancel_error
            raise

    async def send(self, callback, block):
        t0 = time.perf_counter()
        await callback({"blockAddedNotification": {"block": block}})
        self.callback_times.append(time.perf_counter() - t0)


class FakeMultiClient(object):
    htnd = None

    async def _get_or_initialize_htnd(self):
        return self.htnd


sio = FakeSio()
server = types.ModuleType("server")
server.sio, server.htnd_client = sio, FakeMultiClient()
htnd_pkg, htnd_thread = types.ModuleType("htnd"), types.ModuleType("htnd.HtndThread")
htnd_thread.HtndThread = FakeHtndThread
sys.modules.update({"server": server, "htnd": htnd_pkg, "htnd.HtndThread": htnd_thread})

_path = os.path.join(os.path.dirname(__file__), "..", "sockets", "blocks.py")
_spec = importlib.util.spec_from_file_location("sockets_blocks", _path)
blocks = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(blocks)

_spec = importlib.util.spec_from_file_location(
    "helper_queue_logging", os.path.join(os.path.dirname(__file__), "..", "helper", "QueueLogging.py"))
queue_logging = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(queue_logging)


class Recorder(logging.Handler):
    """Collects records; with a `gate` every record waits for it like a stuck log sink would."""

    def __init__(self, gate=None, level=logging.WARNING):
        super().__init__(level)
        self.gate, self.records, self.cond = gate, [], threading.Condition()

    def emit(self, record):
        if self.gate:
            self.gate.wait(10)
        with self.cond:
            self.records.append(record)
            self.cond.notify_all()

    @property
    def messages(self):
        return [r.getMessage() for r in list(self.records)]

    def dropped_total(self):
        return sum(int(n) for m in self.messages for n in re.findall(r"dropped (\d+) oldest", m))

    async def wait_for(self, predicate, timeout=10):
        """Waits for `predicate(self)`, woken by each record; False after `timeout`."""
        def wait():
            with self.cond:
                return self.cond.wait_for(lambda: predicate(self), timeout)
        return await asyncio.to_thread(wait)


def header_only(i, with_txs=False):
    block = {"header": {"blueScore": str(i)}, "verboseData": {"hash": "h%d" % i, "transactionIds": ["tx"]}}
    if with_txs:
        block["transactions"] = [{"verboseData": {"transactionId": "tx"}}]
    return block


class BlockQueueTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        sio.emitted, sio.emit_delay = [], 0
        FakeHtndThread.instances, FakeHtndThread.fail_on_enter = [], False
        FakeHtndThread.close_error = FakeHtndThread.gate = FakeHtndThread.started = None
        self.rec = Recorder()
        blocks.logger.addHandler(self.rec)
        self.addCleanup(blocks.logger.removeHandler, self.rec)

    async def run_config(self, script):
        server.htnd_client.htnd = FakeHtnd(script)
        await asyncio.wait_for(blocks.config(), 10)
        return server.htnd_client.htnd

    async def burst(self, htnd, callback):
        await callback({"notifyBlockAddedResponse": {}})
        for i in range(BURST):
            await htnd.send(callback, header_only(i))
        deadline = time.monotonic() + 10
        while len(sio.emitted) < blocks.BLOCK_QUEUE_SIZE and time.monotonic() < deadline:
            await asyncio.sleep(0.01)

    def cleanup_failures(self):
        return [r for r in self.rec.records if r.getMessage() == "Block subscription cleanup failed"]

    async def test_burst_keeps_newest_in_order(self):
        htnd = await self.run_config(self.burst)  # no extra yield: cleanup must be complete on return

        times = htnd.callback_times
        thread = FakeHtndThread.instances[0]
        print("\ncallbacks=%d max_callback_ms=%.3f dropped=%d emitted=%d hydrated=%d channels=%d closed=%s" % (
            len(times), max(times) * 1000, self.rec.dropped_total(), len(sio.emitted), len(thread.hydrated),
            len(FakeHtndThread.instances), thread.closed))  # diagnostic only: timing is not asserted

        first = BURST - blocks.BLOCK_QUEUE_SIZE
        self.assertEqual(len(times), BURST)
        self.assertEqual([b["block_hash"] for b in sio.emitted], ["h%d" % i for i in range(first, BURST)])  # newest kept, in order
        self.assertEqual(self.rec.dropped_total(), first)  # every drop is counted in a log line
        self.assertEqual(set(thread.hydrated), {"h%d" % i for i in range(BURST - blocks.HYDRATE_MAX_BACKLOG, BURST)})
        self.assertEqual(sio.emitted[-1]["txs"][0]["txId"], "tx")
        self.assertEqual(sio.emitted[0]["txs"], [])  # backlog: header + txCount only
        self.assertEqual(sio.emitted[0]["txCount"], 1)
        self.assertEqual(len(FakeHtndThread.instances), 1)  # one reused channel
        self.assertEqual(thread.host, htnd.htnd_host)  # hydration uses the node that serves the notifications
        self.assertTrue(thread.closed)  # closed before config() returned
        self.assertFalse(any("Block subscription ended" in m for m in self.rec.messages))  # nothing left over
        self.assertEqual(self.cleanup_failures(), [])  # cancelling the children is not a failure

    async def test_callbacks_do_not_wait_for_blocked_hydration(self):
        FakeHtndThread.gate, FakeHtndThread.started = asyncio.Event(), asyncio.Event()

        async def script(htnd, callback):
            await htnd.send(callback, header_only(0))
            await asyncio.wait_for(FakeHtndThread.started.wait(), 5)  # the worker is inside a blocked getBlock
            for i in range(1, 50):
                await htnd.send(callback, header_only(i))  # a callback awaiting hydration would never return
            FakeHtndThread.gate.set()
            while len(sio.emitted) < 50:
                await asyncio.sleep(0.01)

        await self.run_config(script)  # the outer timeout is the deadlock detector
        self.assertEqual([b["block_hash"] for b in sio.emitted], ["h%d" % i for i in range(50)])

    async def test_drops_reported_after_drain_without_further_overflow(self):
        first = BURST - blocks.BLOCK_QUEUE_SIZE

        async def burst_then_idle(htnd, callback):
            await self.burst(htnd, callback)
            # the stream stays open and nothing overflows again: the pending count must still be reported
            self.assertTrue(await self.rec.wait_for(lambda rec: rec.dropped_total() == first))

        await self.run_config(burst_then_idle)
        self.assertEqual(self.rec.dropped_total(), first)

    async def test_stuck_log_sink_does_not_stall_event_loop(self):
        gate = threading.Event()
        stuck = Recorder(gate, logging.DEBUG)
        counter = Recorder(level=logging.DEBUG)  # sees every record synchronously
        blocks.logger.addHandler(counter)
        self.addCleanup(blocks.logger.removeHandler, counter)
        old_level = blocks.logger.level
        blocks.logger.setLevel(logging.INFO)  # production default: per-block INFO records are emitted
        self.addCleanup(blocks.logger.setLevel, old_level)
        stop = queue_logging.start_queue_logging(blocks.logger, stuck, maxsize=50)
        try:
            await self.run_config(self.burst)  # hangs (outer timeout) if any log call waits for the sink
        finally:
            gate.set()
            await asyncio.to_thread(stop)
        reports = [m for m in stuck.messages if m.startswith("Log queue full")]
        dropped = sum(int(re.search(r"dropped (\d+) log", m).group(1)) for m in reports)
        self.assertGreater(dropped, 0)
        self.assertEqual(len(stuck.messages) - len(reports) + dropped, len(counter.records))  # nothing lost unreported
        self.assertEqual(len(sio.emitted), blocks.BLOCK_QUEUE_SIZE)

    async def test_worker_failure_reaches_config_and_stops_reader(self):
        FakeHtndThread.fail_on_enter = True

        async def never(htnd, callback):
            await asyncio.Event().wait()

        with self.assertRaises(OSError):
            await self.run_config(never)
        self.assertTrue(server.htnd_client.htnd.cancelled)

    async def test_stream_end_reports_discarded_and_in_flight(self):
        sio.emit_delay = 0.05  # worker is inside emit when the stream ends

        async def three_then_eof(htnd, callback):
            for i in range(3):
                await htnd.send(callback, header_only(i, with_txs=True))

        await self.run_config(three_then_eof)
        ended = [m for m in self.rec.messages if "Block subscription ended" in m]
        self.assertEqual(len(ended), 1)
        self.assertIn("discarded_pending=2 first_hash=h1 last_hash=h2 in_flight_hash=h0", ended[0])
        self.assertTrue(FakeHtndThread.instances[0].closed)

    async def test_stream_error_waits_for_channel_close(self):
        async def fail(htnd, callback):
            await htnd.send(callback, header_only(0, with_txs=True))
            raise RuntimeError("stream broke")

        with self.assertRaises(RuntimeError):
            await self.run_config(fail)
        self.assertTrue(FakeHtndThread.instances[0].closed)

    async def test_failed_channel_close_is_logged_after_clean_eof(self):
        error = FakeHtndThread.close_error = OSError("close failed")

        async def one_then_eof(htnd, callback):
            await htnd.send(callback, header_only(0, with_txs=True))

        await self.run_config(one_then_eof)  # no primary failure: config returns, the close error is visible
        failures = self.cleanup_failures()
        self.assertEqual(len(failures), 1)
        self.assertIs(failures[0].exc_info[1], error)
        self.assertTrue(FakeHtndThread.instances[0].closed)

    async def test_primary_failure_stays_the_raised_one_when_close_also_fails(self):
        error = FakeHtndThread.close_error = OSError("close failed")

        async def fail(htnd, callback):
            await htnd.send(callback, header_only(0, with_txs=True))
            raise RuntimeError("stream broke")

        with self.assertRaises(RuntimeError):
            await self.run_config(fail)
        self.assertEqual([r.exc_info[1] for r in self.cleanup_failures()], [error])

    async def test_failed_reader_close_is_logged_and_worker_still_stopped(self):
        FakeHtndThread.fail_on_enter = True  # primary failure: the worker never starts
        error = OSError("reader close failed")

        async def never(htnd, callback):
            htnd.cancel_error = error
            await asyncio.Event().wait()

        with self.assertRaises(OSError) as raised:
            await self.run_config(never)
        self.assertEqual(str(raised.exception), "cannot create channel")  # primary, not the cleanup error
        self.assertEqual([r.exc_info[1] for r in self.cleanup_failures()], [error])
        self.assertTrue(server.htnd_client.htnd.cancelled)


class QueueLoggingTest(unittest.TestCase):
    def test_full_queue_drops_without_blocking_and_reports_on_next_enqueue(self):
        log_queue = queue.Queue(2)
        handler = queue_logging.DropCountingQueueHandler(log_queue)
        logger = logging.Logger("queue-test")
        logger.addHandler(handler)
        for i in range(5):
            logger.warning("m%d", i)  # would block forever on a plain put
        self.assertEqual((handler.dropped, log_queue.qsize()), (3, 2))
        log_queue.get_nowait()
        log_queue.get_nowait()
        logger.warning("m5")
        self.assertEqual([log_queue.get_nowait().getMessage() for _ in range(2)],
                         ["m5", "Log queue full; dropped 3 log records"])
        self.assertEqual(handler.dropped, 0)

    def test_stop_with_a_stalled_sink_returns(self):
        gate = threading.Event()
        sink = Recorder(gate, logging.DEBUG)
        logger = logging.Logger("stop-test")
        old_timeout, queue_logging.STOP_TIMEOUT = queue_logging.STOP_TIMEOUT, 0.2
        self.addCleanup(setattr, queue_logging, "STOP_TIMEOUT", old_timeout)
        stop = queue_logging.start_queue_logging(logger, sink, maxsize=1)
        for i in range(5):
            logger.warning("m%d", i)
        t0 = time.monotonic()
        stop()  # the sink is still stalled: must give up instead of hanging
        self.assertLess(time.monotonic() - t0, 5)
        gate.set()


if __name__ == "__main__":
    unittest.main()
