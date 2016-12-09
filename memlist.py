#!/usr/bin/python

import logging
import socket
import sys
import threading
import pickle
import time

class member_list:
    # Initialize Membership List
    def __init__(self):
        self.lst = []
        self.timestamps = []
        self.lock = threading.Lock()
        # Introducer Static Host and Port
        self.ihost = '172.22.148.4'
        self.iport = 10011

    # Set timestamps
    def init(self, lst, ts):
        with self.lock:
            self.lst = lst
            self.timestamps = ts

    # Add member to membership list
    def add(self, addr, timestamp):
        with self.lock:
            self.lst.append(addr)
            self.timestamps.append(timestamp)

    # Remove member from membership list
    def remove(self, addr):
        with self.lock:
            try:
                idx = self.lst.index(addr)
                del self.lst[idx]
                del self.timestamps[idx]
            except ValueError as err:
                logging.exception(err)

    # Convert mlist to string
    def __str__(self):
        lst = self.lst
        ts = self.timestamps
        rep =  '===== begin mlist =====\n'
        for i in range(len(self.lst)):
            rep += '%s:%s at %s\n' %(lst[i]['host'], lst[i]['port'], ts[i])
        rep += '===== end mlist =====\n'
        return rep
