#!/usr/bin/python

from memlist import *
from util import *
from failure_detector import FailureDetector
from leader_election import *

# Initialize Drone
class drone(threading.Thread):

    def __init__(self, mlist, host, port,fail_queue,elect_queue,init_queue,introducer=False):
        super(drone, self).__init__()
        self.mlist = mlist
        self.intro = introducer
        self.host = host
        self.port = port
        # Fail detect thread handler
        self.fail_detect = None
        # Queue to expose failed node
        self.fail_queue = fail_queue 
        # Leader elect thread handler
        self.leader_elect = None
        # Queue to expose elected leader
        self.elect_queue = elect_queue
        # Leader is none at start-up
        self.init_queue = init_queue
        
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind((self.host,self.port))
        except (socket.error, socket.gaierror) as err_msg:
            logging.exception(err_msg)
            self.sock.close()

    # Join cluster
    def handle_join(self, msg):
        mel = {'host':msg['host'], 'port':msg['port']}
        # Add to membership list
        if self.intro:
            if self.leader_elect is not None:
                leader = self.leader_elect.leader
            else:
                leader = None 
            self.mlist.add(mel, msg['time'])
            msg_init = {
                'cmd': 'init',
                'mlist': self.mlist.lst,
                'timestamps': self.mlist.timestamps,
                'current_leader': leader
            }
            # Update list
            unicast(msg['host'], msg['port'], pickle.dumps(msg_init))

    # Leave cluster
    def handle_leave(self, msg):
        mel = {'host':msg['host'], 'port':msg['port']}
        # Remove from membership list
        self.mlist.remove(mel)
        
    # Start failure detector
    def init_faildetect(self):
        self.fail_detect = FailureDetector(self.mlist,self.host, self.port,self.fail_queue)
        self.fail_detect.run()
        
   # Start leader election 
    def init_leader_election(self):
        # Start the leader thread in the background as soon as node joins
        self.leader_elect = LeaderElection(self.mlist,self.elect_queue)
        self.leader_elect.run()

    # Start Contact
    def run(self):
        while True:
            msg, addr = self.sock.recvfrom(8129)
            rcv_msg = pickle.loads(msg)
            if rcv_msg['cmd'] == 'join' or rcv_msg['cmd'] == 'leave':
                if rcv_msg['cmd'] == 'join':
                    self.handle_join(rcv_msg)
                else:
                    self.handle_leave(rcv_msg)
                logging.info("Membership List Update: " + str(self.mlist))
            elif rcv_msg['cmd'] == 'init':
                self.mlist.init(rcv_msg['mlist'], rcv_msg['timestamps'])
                self.init_queue.put(rcv_msg['current_leader'])
                self.init_leader_election()
                self.init_faildetect()
            elif rcv_msg['cmd'] == 'ping':
                self.fail_detect.recv_ping(rcv_msg['data'], self.sock, addr,
                                               '%s/%d/%s' %(rcv_msg['sender_host'],
                                                            rcv_msg['sender_port'],
                                                            rcv_msg['sender_timestamp']))
            elif rcv_msg['cmd'] == 'maple':
                self.task_queue.put(rcv_msg)
            elif rcv_msg['cmd'] == 'juice':
                self.task_queue.put(rcv_msg)
