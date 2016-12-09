#!/usr/bin/env python

# Import from node and console
from node import *
from console import *
import Queue


# Start logging
if __name__ == '__main__':
    logging.basicConfig(filename="intro.log", level=logging.INFO, filemode="w")
    host = socket.gethostbyname(socket.gethostname())
    port = 10011
    # Communication between FailDetector and other threads is via Queue
    fail_queue = Queue.Queue()
    # Initialize membership list
    mlist = member_list()
    # Communicate elected Master between threads
    elect_queue = Queue.Queue()
    # Communicate between console and drone on initial master
    init_queue = Queue.Queue()
    # Task Queue
    task_queue = Queue.Queue()
    # Initialize drone
    drn = drone(mlist, host, port,fail_queue,elect_queue,init_queue,introducer=True)
    drn.task_queue = task_queue
    drn.start()
    # Initialize console
    cc = console_client(mlist, host, port,fail_queue,elect_queue,init_queue,task_queue,introducer=True)
    cc.task_queue = task_queue
    cc.start()
