import logging
import time
from queue import Queue, LifoQueue, PriorityQueue

from multithreading import MultithreadingQueue, PersistentMultithreadingQueue, PriorityItem

logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-9s) %(message)s',)
logger = logging.getLogger(__name__)


# Dummy functions to show multiple possible tasks.
def function_path_1(numbers: list):
    logging.debug("Starting function_path_1.")
    print_hello_world()
    logging.debug("Pausing for 2 seconds")
    time.sleep(2)
    sum_numbers(numbers)
    multiply_numbers(numbers)
    logging.debug("Finishing function_path_1.")


def function_path_2(string: str, count: int):
    logging.debug("Starting function_path_2.")
    print_string(string)
    logging.debug("Pausing for 2 seconds")
    time.sleep(2)
    count_to_value(count)
    logging.debug("Finishing function_path_2.")


def print_hello_world():
    logging.debug("Starting print_hello_world.")
    logging.info("hello world")
    logging.debug("Finishing print_hello_world.")


def sum_numbers(numbers: list):
    logging.debug("Starting sum_numbers.")
    logging.info(f"Supplied numbers are: {numbers}")
    my_sum = sum(numbers)
    logging.info(f"The sum is: {my_sum}")
    logging.debug("Finishing sum_numbers.")


def multiply_numbers(numbers: list):
    logging.debug("Starting multiply_numbers.")
    logging.info(f"Supplied numbers are: {numbers}")
    if len(numbers) == 0:
        product = None
    else:
        product = numbers[0]
        for index in range(1, len(numbers)):
            product *= numbers[index]
    logging.info(f"The product is: {product}")
    logging.debug("Finishing multiply_numbers.")


def print_string(string: str):
    logging.debug("Starting print_string.")
    logging.info(f"Printing string: {string}")
    logging.debug("Finishing print_string.")


def count_to_value(value: int):
    logging.debug("Starting count_to_value.")
    for index in range(value):
        logging.info(index)
    logging.debug("Finishing count_to_value.")


def lock_thread(obj, value: int):
    logging.debug("Starting lock_thread.")
    logging.info(f"Locking thread for: {value} seconds.")
    obj.lock.acquire()
    time.sleep(value)
    obj.lock.release()
    logging.info(f"Releasing thread for.")
    logging.debug("Finishing lock_thread.")


# Controls which demos are run.
run_normal = False
run_persistent = False
priority_q = True

# Initialize data to pass to the dummy functions.
tasks_1 = [
    {"target": function_path_1, "args": ([6, 5, 7],)},
    {"target": function_path_1, "args": ([100, 200, 300],)},
    {"target": function_path_2, "args": ("Good bye!!", 20)},
    {"target": count_to_value, "args": (30,)},
    {"target": print_hello_world},
    {"target": function_path_2, "args": ("Seperate args and kwargs", ), "kwargs": {"count": 7}},
    {"target": function_path_2, "kwargs": {"string": "Only kwargs", "count": 7}, "name": "Testing kwargs"},
]
tasks_2 = [
    {"target": function_path_1, "args": ([6, 5, 7],), "name": "Bonus tasks, 1!!!"},
    {"target": function_path_1, "args": ([6, 5, 7],), "name": "Bonus tasks, 2!!!"},
    {"target": function_path_1, "args": ([6, 5, 7],), "name": "Bonus tasks, 3!!!"},
]
priority_1 = [5, 4, 10, 3, 100, 5, 8]
priority_2 = [1, 1000, 0]

if run_normal:
    logger.info("Begin MultithreadingQueue demo.")
    multi_threader_1 = MultithreadingQueue(tasks=tasks_1, thread_count=3)
    multi_threader_1.start_threads(join=True)
    time.sleep(1)

    logger.info("Create a new MultithreadingQueue object for another set of tasks.")
    multi_threader_2 = MultithreadingQueue(tasks=tasks_2, thread_count=4)
    multi_threader_2.start_threads(join=True)
    logger.info(
        "This statement isn't run until after all the tasks in multi_threader_2 are done because join was set to True."
    )
    logger.info("Finish MultithreadingQueue demo.")

if run_persistent:
    logger.info("Begin PersistentMultithreadingQueue demo.")
    q = Queue()
    for task in tasks_1:
        q.put(task)
    persistent_multi_threader = PersistentMultithreadingQueue(q=q, thread_count=3)
    persistent_multi_threader.start_threads(join=False)

    logger.info("Add more tasks to the existing PersistentMultithreadingQueue object.")
    persistent_multi_threader.add_to_queue(tasks_2, join=False)
    logger.info(
        "Other work can be done in the main thread while the multi threader is running because join was set to False."
    )
    logger.info(
        "Call q.join() so that the program doesn't end until the queue is empty.  Alternatively join could be set "
        "to True for the last start_threads or add_to_queue call."
    )
    q.join()
    logger.info("Finish PersistentMultithreadingQueue demo.")

if priority_q:
    logger.info("Begin priority queue demo.")
    q_priority = PriorityQueue()
    priority_item = PriorityItem()
    for task, priority in zip(tasks_1, priority_1):
        q_priority.put(priority_item.pack(priority, task))
    priority_multi_threader = PersistentMultithreadingQueue(q=q_priority, thread_count=3)
    priority_multi_threader.start_threads(join=False)

    logger.info("Add high and low priority tasks to the priority queue while the threads are still running.")
    priority_multi_threader.add_to_queue(
        [priority_item.pack(priority, task) for task, priority in zip(tasks_2, priority_2)],
        join=False)
    logger.info(
        "Using the priority_multi_threader object to tell the queue to join before allowing the program to move on."
    )
    priority_multi_threader.q.join()
    logger.info("Finish priority queue demo.")
