# encoding: utf-8
import logging
import logging.handlers
import queue

LOG_QUEUE_SIZE = 10000
STOP_TIMEOUT = 2  # seconds each for enqueueing the stop marker and joining the listener thread


class DropCountingQueueHandler(logging.handlers.QueueHandler):
    """Hands records to a bounded queue without ever blocking; counts records dropped when it is full."""

    def __init__(self, log_queue):
        super().__init__(log_queue)
        self.dropped = 0

    def enqueue(self, record):
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self.dropped += 1  # handle() holds self.lock around emit()
            return
        self.report_dropped()

    def report_dropped(self, timeout=0):
        with self.lock:
            if not self.dropped:
                return
            record = logging.LogRecord(
                __name__, logging.WARNING, __file__, 0, "Log queue full; dropped %s log records", (self.dropped,), None,
            )
            try:
                self.queue.put(record, timeout=timeout)
            except queue.Full:
                return  # reported with a later record
            self.dropped = 0


class _Listener(logging.handlers.QueueListener):
    def stop(self):
        # a stalled sink must not hang shutdown
        if self._thread:
            try:
                self.queue.put(self._sentinel, timeout=STOP_TIMEOUT)
            except queue.Full:
                return
            self._thread.join(STOP_TIMEOUT)
            self._thread = None


def start_queue_logging(logger, sink, maxsize=LOG_QUEUE_SIZE):
    """Routes `logger` through a bounded queue to `sink`, which runs in a listener thread. Returns the stop function."""
    log_queue = queue.Queue(maxsize)
    handler = DropCountingQueueHandler(log_queue)
    listener = _Listener(log_queue, sink)
    logger.addHandler(handler)
    listener.start()

    def stop():
        logger.removeHandler(handler)
        handler.report_dropped(STOP_TIMEOUT)
        listener.stop()

    return stop
