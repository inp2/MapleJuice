import socket
import sys
import logging
import time
import random
import math
import threading
import pickle

# Return membershiplist
def getmid(mlist, i):
    return '%s/%d/%s' %(mlist.lst[i]['host'], mlist.lst[i]['port'], mlist.timestamps[i])

# Get the address and port
def getmel(mid):
    addr = mid.split('/')
    return {'host': addr[0], 'port': int(addr[1])}

class FailureDetector:
    # Initailize Failure Detector Class
    def __init__(self, mlist, host, port,fail_queue):
        self.buffer_recent = {}
        self.mlist = mlist
        self.timestamp = self.mlist.time
        self.host = host
        self.port = port
        self.fail_queue = fail_queue
        #self.crash_fail = []
    # Form a piggyback packet
    def form_piggyback_packet(self,func_identifier,msg_type):
        msg_formed = msg_type
        for key,val in  self.buffer_recent.items():
            #logging.info(func_identifier + ' Reading key from dictionary ' + key)
            if val > 0:
               new_val = val - 1
               msg_formed = msg_formed + ',' + key
               logging.info(func_identifier + ' Form package Fail/new node Information of ' + key)
               self.buffer_recent[key] = new_val

        return msg_formed

    # Update the recently received buffer list
    def update_buffer_list(self,func_identifier, address_id_list):
        size = len(self.mlist.lst)
        if size >= 2:
            dissemination_cnt = int(math.ceil((math.log(size,2))))
        else:
            dissemination_cnt = 1
        for address_id in address_id_list:
            logging.info(func_identifier + ' Check Recent Buffer for ' + address_id)
            if address_id not in self.buffer_recent:
                self.buffer_recent[address_id] = dissemination_cnt
                logging.info(func_identifier + ' Write to dictionary key ' + address_id +  ' value ' + str(dissemination_cnt) )

    # Update the server list
    def update_server_list(self):

       for key,val in self.buffer_recent.items():
           addr = key.split('_')
           mel = getmel(addr[1])

           # 01_failaddressid, 01_nodeleaveid
           # Remove the fail address if it exists in membership list
           if addr[0] == '01':
               if mel in self.mlist.lst:
                   self.mlist.remove(mel)
                   self.fail_queue.put(addr[1])
                   #self.crash_fail.append(addr[1])
                   logging.info('Update membership list with removal of' + addr[1])
           # 10_newnodeid, Add the new node if it is not in membership list already
           elif addr[0] == '10':
               if mel not in self.mlist.lst:
                   #Once a node is detected as fail, it is not added back
                   #if addr[1] not in crash_fail:
                   self.mlist.add(mel, addr[1].split('/')[-1])
                   logging.info('Update membership list with addition of' + addr[1])
           # Garbage collection for buffer_recent
           if val == 0:
               self.buffer_recent.pop(key)

    # Send ping
    def send_ping(self,lock):
        while True:
            lock.acquire()
            self.update_server_list()
            lock.release()
            # Choose a node and shuffle list
            inds = range(len(self.mlist.lst))
            random.shuffle(inds)
            # Check size of membership list
            size_mlist = len(self.mlist.lst)
            # If len is greater than 0
            if size_mlist > 0:
                # Set SWIM established timeout
                # swim_timeout =  2.8/(2*size_mlist - 1)
                swim_timeout =  4*2.8/(2*size_mlist - 1)
            else:
                # Set SWIM established timeout
                swim_timeout = 0.120
            #logging.info('SWIM timeout = ' + str(swim_timeout))
            # for address in self.server_list:
            for idx in inds:
                address = getmid(self.mlist, idx)
                fail_indicator = False
                fail_address = '01_' + address
                # Do not send pings to already fail node
                lock.acquire()
                if fail_address in self.buffer_recent:
                    fail_indicator = True
                lock.release()
                if fail_indicator == True:
                    continue
                # Set UDP Connection
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.settimeout(swim_timeout)
                    lock.acquire()
                    ping_message = self.form_piggyback_packet('send_ping', 'p')
                    lock.release()
                    addr = address.split('/')
                    pkt = pickle.dumps({
                        'cmd': 'ping',
                        'data': ping_message,
                        'sender_host': self.host,
                        'sender_port': self.port,
                        'sender_timestamp': self.timestamp
                    })
                    sock_sent = sock.sendto(pkt, (addr[0],int(addr[1])))
                    data = ''
                    try:
                        ret_buf, server_identity = sock.recvfrom(8192)

                    except socket.timeout:
                        logging.info('ACK not received within timeout from node : ' + address)
                        address_id = []
                        address_id.append('01_' + address)
                        lock.acquire()
                        #logging.info('Update recent buffer from send_ping')
                        self.update_buffer_list('send_ping', address_id)
                        lock.release()
                        # Communicate Fail Address to Master 
                        #self.fail_queue.put(address)
                except (socket.error,socket.gaierror) as err_msg:
                    logging.error("Socket Error")
                    logging.exception(err_msg)
                finally:
                    sock.close()

    # Receive ping
    def recv_ping(self, buf, sock, sender, sender_id):

        lock = self.lock

        ack_message = 'a'
        lock.acquire()
        # Create Acknowledgement
        ack_message = self.form_piggyback_packet('recv_ping', 'a')
        lock.release()
        # Send Acknowledgement
        sock.sendto(ack_message, sender)
        data = buf.split(',')
        lock.acquire()
        # If ping was received from node not in mlist, add it to buffer_list
        smel = getmel(sender_id)
        if smel not in self.mlist.lst:
            data.append('10_' + sender_id)
        #logging.info('recv_ping_debug_statement ' + str(sender[0]))
        self.update_buffer_list('recv_ping', data[1:])
        lock.release()

    

    # Start thread
    def run(self):
        try:
            lock = threading.Lock()
            self.lock = lock
            ping_thread = threading.Thread(target=self.send_ping,args=(lock,))
            ping_thread.daemon = True
            ping_thread.start()
        except(KeyboardInterrupt, SystemExit):
            print("exiting all threads and main program")


# Main Function to connect and start logging
if __name__ == "__main__":
    FORMAT = '%(asctime)-15s  %(message)s'
    logging.basicConfig(format = FORMAT, filename = "faildetector.log", level = logging.INFO, filemode = "w")
    fail_detect = FailureDetector()
    fail_detect.run()
