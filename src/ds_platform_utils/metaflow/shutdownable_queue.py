"""ShutdownableQueue: A thread-safe queue that supports graceful shutdown."""

import queue
import threading
import time
from typing import Optional


class QueueShutdownError(Exception):
    """Raised when a ShutdownableQueue operation is attempted after shutdown."""

    def __init__(self, message: str = "Queue has been shut down", cause: Optional[Exception] = None):
        super().__init__(message)
        self.cause = cause


class ShutdownableQueue:
    """A queue wrapper that can be shut down, causing all blocking operations to raise an exception.

    This is useful for graceful shutdown of producer-consumer pipelines. When shutdown() is called,
    any threads blocked on get() or put() will raise QueueShutdownError, and subsequent operations
    will also raise immediately.

    Example::

        q = ShutdownableQueue(maxsize=1)

        # In producer thread:
        try:
            q.put(data)
        except QueueShutdownError:
            return  # Graceful exit

        # In consumer thread:
        try:
            data = q.get()
        except QueueShutdownError:
            return  # Graceful exit

        # When error occurs in any thread:
        q.shutdown(cause=exception)  # All threads will receive QueueShutdownError

    """

    def __init__(self, maxsize: int = 0):
        """Initialize the queue.

        Args:
            maxsize: Maximum queue size (0 for unlimited)

        """
        self._queue: queue.Queue = queue.Queue(maxsize=maxsize)
        self._shutdown = False
        self._cause: Optional[Exception] = None
        self._lock = threading.Lock()
        self._shutdown_event = threading.Event()

    def shutdown(self, cause: Optional[Exception] = None) -> None:
        """Shut down the queue, causing all operations to raise QueueShutdownError.

        Args:
            cause: Optional exception that caused the shutdown (will be attached to QueueShutdownError)

        """
        with self._lock:
            if not self._shutdown:
                self._shutdown = True
                self._cause = cause
                self._shutdown_event.set()
                # Unblock any waiting threads by putting a sentinel
                try:
                    self._queue.put_nowait(None)
                except queue.Full:
                    pass

    def _check_shutdown(self) -> None:
        """Raise QueueShutdownError if queue is shut down."""
        if self._shutdown:
            raise QueueShutdownError(cause=self._cause)

    def put(self, item, timeout: Optional[float] = None) -> None:
        """Put an item into the queue.

        Args:
            item: Item to put
            timeout: Timeout in seconds (None for infinite)

        Raises:
            QueueShutdownError: If queue is shut down

        """
        self._check_shutdown()
        deadline = time.time() + timeout if timeout else None

        while True:
            self._check_shutdown()
            try:
                # Use short timeout to periodically check shutdown flag
                wait_time = min(0.1, timeout) if timeout else 0.1
                if deadline:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        raise queue.Full
                    wait_time = min(wait_time, remaining)
                self._queue.put(item, timeout=wait_time)
                return
            except queue.Full:
                if deadline and time.time() >= deadline:
                    raise
                continue

    def get(self, timeout: Optional[float] = None):
        """Get an item from the queue.

        Args:
            timeout: Timeout in seconds (None for infinite)

        Returns:
            Item from queue

        Raises:
            QueueShutdownError: If queue is shut down
            queue.Empty: If timeout expires

        """
        self._check_shutdown()
        deadline = time.time() + timeout if timeout else None

        while True:
            self._check_shutdown()
            try:
                # Use short timeout to periodically check shutdown flag
                wait_time = min(0.1, timeout) if timeout else 0.1
                if deadline:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        raise queue.Empty
                    wait_time = min(wait_time, remaining)
                item = self._queue.get(timeout=wait_time)
                # Re-check shutdown after getting item (might be sentinel from shutdown)
                self._check_shutdown()
                return item
            except queue.Empty:
                if deadline and time.time() >= deadline:
                    raise
                continue

    def put_nowait(self, item) -> None:
        """Put an item without blocking.

        Raises:
            QueueShutdownError: If queue is shut down
            queue.Full: If queue is full

        """
        self._check_shutdown()
        self._queue.put_nowait(item)

    def get_nowait(self):
        """Get an item without blocking.

        Raises:
            QueueShutdownError: If queue is shut down
            queue.Empty: If queue is empty

        """
        self._check_shutdown()
        item = self._queue.get_nowait()
        self._check_shutdown()
        return item

    @property
    def is_shutdown(self) -> bool:
        """Return True if queue has been shut down."""
        return self._shutdown
