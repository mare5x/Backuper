"""Base for uploader and downloader modules."""

import queue
import logging
from concurrent.futures import ThreadPoolExecutor


class _Queue(queue.Queue):
    def drain(self):
        while True:
            try:
                self.get_nowait()
                self.task_done()
            except queue.Empty:
                break


def start_queue(fn, n_threads=5, thread_prefix="_loader"):
    """N threads will use 'fn' to process items from a queue, until the queue is empty.
    
    fn: (QItem) -> None.

    If a thread raises an exception, that exception will be raised when calling
    wait_for_queue. The queue will get drained and threads will be stopped
    as soon as they finish processing their current items.
    """
    q = _Queue()
    q.n_threads = n_threads  # A convenience attribute.
    executor = ThreadPoolExecutor(max_workers=n_threads, thread_name_prefix=thread_prefix)
    for i in range(n_threads):
        executor.submit(_queue_worker, q, fn)
    # The resources associated with the executor will be freed when all pending futures are done executing.
    executor.shutdown(wait=False)
    return q

def _queue_worker(q, fn):
    while True:
        item = q.get()
        if item is None:
            q.task_done()
            break

        try:
            fn(item)
        except BaseException as e:
            q.exception = e
            q.drain()
            q.task_done()
            break

        q.task_done()

def wait_for_queue(q, stop=True):
    """q must be a _Queue returned by the start_queue method.
    If 'stop' is True, consider the queue unusable. Associated threads will stop.

    Exceptions raised by threads working the queue will get raised here.
    """
    # Block until all tasks are done.
    q.join()

    exception = getattr(q, "exception", None)

    # Stop worker threads.
    if stop or exception is not None:
        for _ in range(q.n_threads):
            q.put(None)

    if exception is not None:
        logging.error("Error in thread!", exc_info=exception)
        raise exception