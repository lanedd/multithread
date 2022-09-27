# Prompt: Create a class that runs multithreading on a list of dictionaries with functions and argument tuples.

import threading
import logging
from collections import deque, namedtuple
from queue import Queue, LifoQueue, PriorityQueue
from typing import List, Optional, Union


logger = logging.getLogger(__name__)
LOCK = threading.Lock()


class MultithreadingQueue:
    """
    It is NOT recommended to add the queue, _q, after the threads have been started.  Once there are no more tasks
    in the queue the threads will end.  Some (or all) of the threads can end before more tasks are added to the
    queue.  Instead, create a new MultithreadingQueue object with the additional tasks.
    """
    def __init__(
            self,
            tasks: List[dict],
            thread_count: Optional[int] = None,
            queue_type: str = "FIFO",
            concurrent_start: bool = False,
    ):
        self._q = deque(tasks)
        self._thread_count = thread_count
        self.queue_type = queue_type
        self.concurrent_start = concurrent_start
        self.lock = LOCK
        self.threads = list()

    def start_threads(self, join: bool = True):
        if self._thread_count is not None:
            thread_count = self._thread_count
        else:
            thread_count = len(self._q)

        for _ in range(thread_count):
            logger.debug(f"Creating new thread.")
            t = threading.Thread(target=self._run_thread)
            self.threads.append(t)

        if self.concurrent_start:
            self.lock.acquire()
        for t in self.threads:
            t.start()
        if self.concurrent_start:
            self.lock.release()

        if join:
            for t in self.threads:
                t.join()

    def _run_thread(self):
        logger.debug(f"Starting thread: {threading.current_thread().name}.")
        while len(self._q) > 0:
            self._run_task()
        logger.debug(f"Ending thread: {threading.current_thread().name}.")

    def _run_task(self):
        self.lock.acquire()
        if len(self._q) > 0:
            task = self._pop_task()
            self.lock.release()
            if "name" in task:
                logger.info(f"Starting task: {task['name']} on thread: {threading.current_thread().name}.")
            else:
                logger.debug(f"Starting task on thread: {threading.current_thread().name}.")
            args = task.get("args", tuple())
            kwargs = task.get("kwargs", dict())
            task["target"](*args, **kwargs)
            if "name" in task:
                logger.info(f"Finishing task: {task['name']} on thread: {threading.current_thread().name}.")
            else:
                logger.debug(f"Finishing task on thread: {threading.current_thread().name}.")
        else:
            self.lock.release()

    def _pop_task(self):
        if self.queue_type.upper() == "FIFO":
            task = self._q.popleft()
        elif self.queue_type.upper() == "LIFO":
            task = self._q.pop()
        else:
            raise NotImplemented("The desired queue type has not been implemented.")
        return task


class PersistentMultithreadingQueue:
    """"""
    def __init__(
            self,
            q: Union[Queue, LifoQueue, PriorityQueue],
            thread_count: Optional[int] = None,
    ):
        self.q = q
        self._thread_count = thread_count
        self.lock = LOCK
        self.threads = list()

    def start_threads(self, join: bool = True):
        if self._thread_count is not None:
            thread_count = self._thread_count
        else:
            thread_count = self.q.qsize()

        for _ in range(thread_count):
            logger.debug(f"Creating new thread.")
            t = threading.Thread(target=self._run_thread, daemon=True)
            self.threads.append(t)

        for t in self.threads:
            t.start()

        if join:
            self.q.join()

    def _run_thread(self):
        logger.debug(f"Starting thread: {threading.current_thread().name}.")
        while True:
            logger.debug(f"Iterating thread: {threading.current_thread().name}.")
            self._run_task()

    def _run_task(self):
        task = self.q.get()
        # If PriorityItemPackage, unpack the item.
        if isinstance(task, PriorityItem.PriorityItemPackage):
            task = PriorityItem.unpack(task)
        if "name" in task:
            logger.info(f"Starting task: {task['name']} on thread: {threading.current_thread().name}.")
        else:
            logger.debug(f"Starting task on thread: {threading.current_thread().name}.")
        args = task.get("args", tuple())
        kwargs = task.get("kwargs", dict())
        task["target"](*args, **kwargs)
        if "name" in task:
            logger.info(f"Finishing task: {task['name']} on thread: {threading.current_thread().name}.")
        else:
            logger.debug(f"Finishing task on thread: {threading.current_thread().name}.")
        self.q.task_done()

    def add_to_queue(self, tasks, join: bool = True):
        for task in tasks:
            self.q.put(task)

        if join and self.threads:
            self.q.join()


class PriorityItem:
    PriorityItemPackage = namedtuple("PriorityItemHolder", ["priority", "i", "item"])

    def __init__(self, ascending: bool = True):
        self._ascending = ascending
        self.i = 0
        self.lock = LOCK

    def pack(self, priority, item):
        self.lock.acquire()
        priority_item_package = self.PriorityItemPackage(priority=priority, i=self.i, item=item)
        if self._ascending:
            self.i += 1
        else:
            self.i -= 1
        self.lock.release()
        return priority_item_package

    @staticmethod
    def unpack(priority_item_package: PriorityItemPackage):
        priority, i, item = priority_item_package
        return item

# class MultithreadingQueue:
#     def __init__(
#             self,
#             q: Union[Queue, LifoQueue, PriorityQueue],
#             thread_count: Optional[int] = None,
#             join: bool = True,
#             concurrent_start: bool = False,
#             # daemon: bool = None,
#             # If persist_threads is True, use join = True or put in object.q.join() when finished loading queue or
#             # the program migh terminate while the queue is still populated
#             persist_threads: bool = False,
#     ):
#
#         self.q = q
#         self.thread_count = thread_count
#         self.join = join
#         self.concurrent_start = concurrent_start
#         # self.daemon = daemon
#         self.persist_threads = persist_threads
#         self.lock = LOCK
#         self.threads = list()
#
#     def start_multi_threads(self):
#         if self.thread_count is not None:
#             thread_count = self.thread_count
#         else:
#             thread_count = self.q.qsize()
#
#         for _ in range(thread_count):
#             logger.debug(f"Creating new thread.")
#             t = threading.Thread(target=self._run_thread, daemon=self.persist_threads)
#             self.threads.append(t)
#
#         if self.concurrent_start:
#             self.lock.acquire()
#         for t in self.threads:
#             t.start()
#         if self.concurrent_start:
#             self.lock.release()
#
#         if self.join:
#             self.q.join()
#
#     def _run_thread(self):
#         logger.debug(f"Starting thread: {threading.current_thread().name}.")
#         while not self.q.empty() or self.persist_threads:
#             self.lock.acquire()
#             if not self.q.empty():
#                 task = self.q.get()
#                 self.lock.release()
#                 if "name" in task:
#                     logger.info(f"Starting task: {task['name']} on thread: {threading.current_thread().name}.")
#                 else:
#                     logger.debug(f"Starting task on thread: {threading.current_thread().name}.")
#                 args = task.get("args", tuple())
#                 kwargs = task.get("kwargs", dict())
#                 task["target"](*args, **kwargs)
#                 if "name" in task:
#                     logger.info(f"Finishing task: {task['name']} on thread: {threading.current_thread().name}.")
#                 else:
#                     logger.debug(f"Finishing task on thread: {threading.current_thread().name}.")
#             else:
#                 self.lock.release()
#         logger.debug(f"Ending thread: {threading.current_thread().name}.")
#
#     def pop_left_task(self):
#         task = self.tasks.popleft()
#         return task
#
#     def pop_right_task(self):
#         task = self.tasks.pop()
#         return task
