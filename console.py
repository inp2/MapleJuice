#!/usr/bin/env python

from node import *
import os
from os import walk
import subprocess
from sdfs_utils import *
from master import *
import getpass
import Queue
from node_replicate import *
import socket
import threading
import time
import sys
import re

# Hold Task info for Maple and Juice
class task_info:

    def __init__(self, host, msg):
        self.host = host
        self.msg = msg
        self.copy_cmd = ''

# Initialize console and introducer
class console_client(threading.Thread):
    
    def __init__(self, mlist, host, port, fail_queue, elect_queue, init_queue, task_queue ,introducer=False):
        super(console_client, self).__init__()
        self.mlist = mlist
        self.host = host
        self.port = port
        self.intro = introducer
        self.user = getpass.getuser()
        # Value of block set to MB
        self.block_size = 100000000
        self.rep_factor = 1
        self.master_id = None
        self.master = None
        self.sdfs_utils = sdfs_utils()
        # Communication from fail detector
        self.fail_queue = fail_queue       
        # Communication from leader elector
        self.elect_queue = elect_queue
        # Communication from introducer on initial leader
        self.init_queue = init_queue
        # Task queue for map reduce
        self.task_queue = task_queue
        #Port listening for leader election
        self.lport = 10040
        #Port for Master node communication
        self.master_port = 10020
        # Monitor maple tasks
        self.maple_tasks = {}
        # Monitor juice tasks
        self.juice_tasks = {}
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        except (socket.error, socket.gaierror) as err_msg:
            logging.exception(err_msg)
            self.sock.close()

    # Contact the Introducer and Join the Group
    # Get the current leader from the introducer
    def join_group(self):
        # Clean the SDFS storage area when a node joins
        CMD="rm /usr/local/sdfs_mp3/*"       
        ret = subprocess.call(["ssh", str(self.user) + "@" + str(socket.gethostname()), CMD], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Send Join Message
        msg = {
            'cmd':'join',
            'host': self.host,
            'port': self.port,
            'time': time.time()
        }
        self.mlist.time = msg['time']
        snd_msg = pickle.dumps(msg)
        # Send Message
        self.sock.sendto(snd_msg, (self.mlist.ihost,self.mlist.iport))
        logging.info("Node Command: " + msg['cmd'])
        # Wait until membership is obtained from introducer
        while (len(self.mlist.lst) <=0):
            pass
        # Check the init_leader queue, assign master for the new node
        if not self.init_queue.empty():
            ip_add = self.init_queue.get()
            if ip_add is not None:
                self.master_id = (ip_add,self.master_port)
        # Start leader wait thread after process joins
        leader_wait = threading.Thread(target=self.wait_for_leader)
        leader_wait.daemon = True
        leader_wait.start()
        # Start node_replicate thread as soon as node joins
        replica_thread = node_replicate()
        replica_thread.daemon = True
        replica_thread.start()
         
    # Contact the Introducer and Leave the Group
    def leave_group(self):
        # Send Leave Message
        msg = {'cmd': 'leave', 'host': self.host, 'port': self.port}
        self.mlist.leave()
        logging.info("Node Command: " + msg['cmd'])
    
 
    # Start leader election 
    def initial_leader_election(self,election_id):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node_id = ('', int(self.lport))
            sock.connect(node_id)
            send_pkt = pickle.dumps({'cmd':'initiate', 'id' : election_id}) 
            send_msg = (send_pkt)
            sock.send(send_msg)
            ret = ''
            try:
                data = sock.recv(8192)
                ret = ret + data
            except socket.timeout:
                logging.info("Socket Timeout in Client")
            sock.close()
        except (socket.error,socket.gaierror) as err_msg:
            logging.info(str(err_msg))  
    
    # Wait for leader election
    def wait_for_leader(self):
        while True:
            if self.master_id is None:        
                # Wait for election to complete
                master_ip = self.elect_queue.get()
                # Set master id for SDFS operations            
                self.master_id = (master_ip,int(self.master_port))
                current_ip = socket.gethostbyname(socket.gethostname())
                #If current node is Master, start the Master thread
                if (current_ip == master_ip):
                    self.master = master_node(self.mlist,self.fail_queue)
                    self.master.task_queue = self.task_queue
                    self.master.run()
                # Send the SDFS information to Master to populate metadata
                file_list = self.store()
                # Call Master after a delay, similar to exponential back-off
                ip_split = current_ip.split('.');
                node_id= ip_split[len(ip_split)-1];
                # Sleep to allow for killing of tasks
                time.sleep((int(node_id)/15.0));
                self.sdfs_utils.node_info_handler(file_list,self.master_id)
            else:
                # If current node is not master, look for master failure
                if (socket.gethostbyname(socket.gethostname()) != self.master_id[0]):
                    # Wait for fail information 
                    fail_ip = self.fail_queue.get() 
                    queue_info = (fail_ip).split('/')
                    # If detected fail is a Master:
                    if (queue_info[0] == self.master_id[0]):
                        # Sleep for 20s to eliminate false fail
                        time.sleep(20)
                        lst = self.mlist.lst
                        # Check if node is removed from mlist
                        for i in range(len(lst)):
                            host = lst[i]['host']
                            if (host == queue_info[0]):
                                false_fail = 1
                                break
                            else:
                                false_fail = 0
                        if (false_fail == 0):
                            # Set master to none
                            self.master_id = None
                            # Initiate new election
                            self.initial_leader_election(queue_info[0])
    
    # Contact the Master and Place the File
    def put(self, lfle, sfle):
        if self.master_id is not None:
            logging.info("SDFS Command: Puts")
            # Use Helper Function to handle put
            self.sdfs_utils.put_handler(lfle,sfle,self.block_size,self.rep_factor,self.master_id,self.user)
            logging.info("Put: " + str(lfle))
        else:
            logging.info("Master not elected, hold SDFS operations")

    # Contact the Master and Get the File
    def get(self, sfle, lfle):
        if self.master_id is not None: 
            logging.info("SDFS Command: Get")
            # Contact Master, Determine Best Node
            self.sdfs_utils.get_handler(lfle,sfle,self.master_id,self.user) 
            logging.info("Get: " + str(sfle))
        else:
           loggin.info("Master not elected, hold SDFS operations")

    # Contact the Master, Get metadata of file and delete file from SDFS
    def delete(self, sfle):
        if self.master_id is not None: 
            logging.info("SDFS Command: Delete")
            # Use Helper Function to Handle Delete
            self.sdfs_utils.delete_handler(sfle,self.master_id,self.user)
        else:
           logging.info("Master not elected, hold SDFS operations")

    # Get list of files from this machine
    def store(self):
        logging.info("SDFS Command: Store")
        f = []
        # Go through set directory and get all the files
        for (dirpath, dirnames, filenames) in walk('/usr/local/sdfs_mp3/'):
            f.extend(filenames)
        # Print files on machines
        return f

    # Handle list command
    def lstaddrs(self, sdfsfn):
        if self.master_id is not None: 
            logging.info("SDFS Command: List VM Addresses")
            # Get metadata of sdfs from Master
            file_mdata = self.sdfs_utils.get_metadata(sdfsfn,self.master_id)
            # Display metadata to user
            for key,val in file_mdata.items():
                print key, val
        else:
           logging.info("Master not elected, hold SDFS operations")
        
    # Send maple tasks
    def send_maple_tasks(self, maple_exe, maple_file, num_maples, sdfs_prefix):
        # Hold Juice tasks
        self.juice_tasks = {}
        # Hold maple tasks
        self.maple_tasks = {}
        if self.master_id is not None:
            logging.info("MapleJuice Command: Put Localfile to Master")
            master = self.master_id
            # Number of lines of file
            num_lines = sum(1 for line in open(maple_file))
            if len(self.mlist.lst) < int(num_maples):
                logging.info("Error: Too Few Machines for Tasks")
            tasks = self.mlist.lst[:num_maples]
            for tid in range(num_maples):
                # Create a message for Maple tasks
                msg = {}
                msg['tid'] = tid
                msg['cmd'] = 'maple'
                msg['num_maples'] = num_maples
                msg['maple_exe'] = maple_exe
                msg['maple_file'] = maple_file
                msg['prefix'] = sdfs_prefix
                start = tid*num_lines / num_maples
                end = (tid+1)*num_lines / num_maples
                start += 1
                end += 1
                size = end - start
                # copy file segment to host
                host = tasks[tid]['host']
                copy_cmd = 'tail -n +{0} {1} | head -n {2} | ssh ipalmer2@{3} "cat > ~/MapReduce_Framework/tmp/{4}-{1}"'.format(start,
                                                                                                                                maple_file,
                                                                                                                                size, 'host',
                                                                                                                                sdfs_prefix)
                os.system(copy_cmd.replace('host', host))
                task = task_info(tasks[tid], msg)
                task.copy_cmd = copy_cmd
                self.maple_tasks[host] = task
                # Start task
                self.start_task(tasks[tid], msg)
        else:
            print "Master has not been elected, hold MapleJuice operations"
            logging.info("Master not elected, hold MapleJuice operations")

    # Start task                                                         
    def start_task(self, addr, msg):
        host = addr['host']
        port = addr['port']
        send_pkt = pickle.dumps(msg)
        # Send information to start task
        self.sock.sendto(send_pkt, (host, port))
        
    # Send Juice Tasks
    def send_juice_tasks(self, juice_exe, num_juices, sdfs_dest_filename, partition, delete_input):
        self.juice_tasks = {}
        self.maple_tasks = {}

        if len(self.mlist.lst) < int(num_juices):
            logging.info("Error: Two Few Machines for Tasks")
        tasks = self.mlist.lst[:num_juices]
        # Create Global File List
        gflst = []
        if self.master.metadata.file_list:
            for key, value in self.master.metadata.file_list.iteritems():
                for key, val in value.iteritems():
                    gflst.append(key)
        if partition == 'hash':
            hash_dict = {}
            # For the files from maple phase
            for item in gflst:
                hash_num = abs(hash(item)) & int(sys.maxint) % num_juices
                if hash_num in hash_dict:
                    hash_dict[hash_num].append(item)
                else:
                    hash_dict[hash_num] = [item]
            for tid in hash_dict:
                msg = {}
                msg['cmd'] = "juice"
                msg['num_juices'] = num_juices
                msg['juice_exe'] = juice_exe
                msg['dest_file'] = sdfs_dest_filename
                msg['delete'] = delete_input
                msg['filelist'] = hash_dict[tid]
                task = task_info(tasks[tid], msg)
                self.juice_tasks[tasks[tid]['host']] = task
                self.start_task(tasks[tid], msg)
        elif partition == 'range':
            for tid in range(num_juices):
                start = tid*len(gflst) / num_juices
                end = (tid+1)*len(gflst) / num_juices
                start += 1
                end += 1
                size = end - start
                msg = {}
                msg['cmd'] = "juice"
                msg['num_juices'] = num_juices
                msg['juice_exe'] = juice_exe
                msg['dest_file'] = sdfs_dest_filename
                msg['delete'] = delete_input
                msg['filelist'] = gflst[start:end+1]
                task = task_info(tasks[tid], msg)
                self.juice_tasks[tasks[tid]['host']] = task
                self.start_task(tasks[tid], msg)
        else:
            logging.info("Master not elected, hold MapleJuice operations")

    # Spawn Maple Task
    def spawn_maple_task(self, msg):
        logging.info("Running Map Task: Maple " + msg['maple_exe'])
        import time
        # wait until file segment is here
        file_seg = 'tmp/{0}-{1}'.format(msg['prefix'], msg['maple_file'])
        while not os.path.isfile(file_seg):
            time.sleep(1)
        with open(file_seg) as file_seg_h:
            if os.path.isfile('./' + msg['maple_exe']):
                map_proc = subprocess.Popen(msg['maple_exe'],
                                        stdin=file_seg_h,
                                        stdout=subprocess.PIPE)
                map_out = map_proc.stdout.read()
                # fileDict = {}
                for line in map_out.split('\n'):
                    # newline = re.sub('[^A-Za-z]+', '', line.lower())
                    key = line.split(' ')[0]
                    # Check if key is in fileDict
                    with open('tmp/' + msg['prefix'] + '-' + key, 'a') as fh:                            
                        fh.write(line + "\n")

                    # if key in fileDict:
                    #     with open(fileDict[key], 'a') as fh:                            
                    #         fh.write(line + "\n")
                    # else:
                    #     fileDict[key] = 'tmp/' + msg['prefix'] + '-' + key               
                cmd = 'rm tmp/' + msg['prefix'] + '-' + msg['maple_file']
                os.system(cmd)
                # Put files in sdfs
                f = []
                print 'sleeping'
                time.sleep(10)
                print 'putting'
                # Go through set directory and get all the files
                for (dirpath, dirnames, filenames) in walk('tmp/'):
                    f.extend(filenames)
                for fname in f:
                    self.put('tmp/' + fname,fname)
                cmd = 'rm tmp/*'
                os.system(cmd)
                print "Maple Task Complete"
                logging.info("Maple Task Complete")
            else:
                logging.info(msg['maple_exe'] + " : File Does Not Exist")
                print msg['maple_exe'] + " : File Does Not Exist"

    def spawn_juice_task(self, msg):
        logging.info("Running Juice Task")
        # Get all the files given by partition
        for fname in msg['filelist']:
            fh = fname.split("_block")[0]
            self.get(fh, "tmp/" + fh)
        # Remove all unecessary files
        cmd = 'rm tmp/*_block1'
        os.system(cmd)
        # Get all the files from tmp
        f = []
        # Go through set directory and get all the files
        with open(msg['dest_file'], 'w') as fh:
            for (dirpath, dirnames, filenames) in walk('tmp/'):
                f.extend(filenames)
                for fname in f:
                    with open("tmp/" + fname) as file_seg_h:
                        if os.path.isfile('./' + msg['juice_exe']):
                            map_proc = subprocess.Popen(msg['juice_exe'],
                                                        stdin=file_seg_h,
                                                        stdout=subprocess.PIPE)
                            map_out = map_proc.stdout.read()
                            fh.write(map_out)

        self.put(msg['dest_file'],msg['dest_file'])

        # Remove files
        cmd = 'rm tmp/*'
        os.system(cmd)
        if int(msg['delete']) == 1:
            file_list = self.store()
            for item in file_list:
                fh = item.split("_block1")[0]
                self.delete(fh)
            print "Juice Task Complete: Delete"
        else:
            print "Juice Task Complete"
                        
    def task_watch(self):
        while True:
            if not self.task_queue.empty():
                task = self.task_queue.get()
                if task['cmd'] == 'maple':
                    self.spawn_maple_task(task)
                elif task['cmd'] == 'juice':
                    self.spawn_juice_task(task)
                elif task['cmd'] == 'fail':
                    tlist = None
                    if self.maple_tasks:
                        tlist = self.maple_tasks
                    else:
                        tlist = self.juice_tasks
                    tinfo = tlist[task['host']]
                    def find_free(task_list):
                        for addr in self.mlist.lst:
                            if not addr['host'] in task_list:
                                return addr
                    replacement = find_free(tlist)
                    tlist[replacement['host']] = tinfo
                    os.system(tinfo.copy_cmd.replace('host', replacement['host']))
                    self.start_task(replacement, tinfo.msg)

    # Prompt user of questions
    def run(self):
        task_watcher = threading.Thread(target=self.task_watch)
        task_watcher.start()
        prompt = '()==[:::::::::::::> '
        if self.intro:
            prompt = '[intro] ' + prompt
        while True:
            cmds = raw_input(prompt)
            cmd = cmds.split(" ")
            if cmd[0] == 'put':
                if len(cmd) < 3:
                    print "Incorrect Command"
                    print "put <localfilename> <sdfsfilename>"
                else:
                    self.put(cmd[1], cmd[2])
            elif cmd[0] == 'get':
                if len(cmd) < 3:
                    print "Incorrect Command"
                    print "get <sdfsfilename> <localfilename>"
                else:
                    self.get(cmd[1], cmd[2])
            elif cmd[0] == 'store':                
                file_list = self.store()
                print file_list
            elif cmd[0] == 'delete':
                if len(cmd) < 2:
                    print "Incorrect Command"
                    print "delete sdfsfilename"
                else:
                    self.delete(cmd[1])
            elif cmd[0] == 'ls':
                if len(cmd) < 2:
                    print "Incorrect Command"
                    print "ls <sdfsfilename>"
                else:
                    self.lstaddrs(cmd[1])
            elif cmd[0] == 'join':
                self.join_group()
            elif cmd[0] == 'lm':
                print self.mlist
            elif cmd[0] == 'li':
                print socket.gethostbyname(socket.gethostname())
            elif cmd[0] == 'maple':
                if len(cmd) < 5:
                    print "Incorrect Command"
                    print "maple <maple_exe> <maple_file> <num_maples> <sdfs_prefix>"
                else:
                    maple_exe = cmd[1]
                    maple_file = cmd[2]
                    num_maples = eval(cmd[3])
                    sdfs_prefix = cmd[4]
                    self.send_maple_tasks(maple_exe, maple_file, num_maples, sdfs_prefix)
            elif cmd[0] == 'juice':
                if len(cmd) < 6:
                    print "Incorrect Command"
                    print "juice <juice_exe> <num_juices> <dest_filename> <partition> <delete_input>"
                else:
                    juice_exe = cmd[1]
                    num_juices = eval(cmd[2])
                    dest_filename = cmd[3]
                    partition = cmd[4]
                    delete_input = cmd[5]
                    self.send_juice_tasks(juice_exe, num_juices, dest_filename, partition, delete_input)
            elif cmd[0] == 'leader':
                print str(self.master_id)
            elif cmd[0] == 'elect_leader':
                self.initial_leader_election('000.000.000.000')
            elif cmd[0] == 'exit':
                import os
                os._exit(0)
            else:
                print 'invalid command!'


if __name__ == '__main__':
    # Start logging
    logging.basicConfig(filename="node.log", level=logging.INFO, filemode="w")
    # Set host and port
    host = socket.gethostbyname(socket.gethostname())
    port = 10013
    # Communicate between FailDetector and other threads is via Queue
    fail_queue = Queue.Queue()
    # Communicate elected Master between threads
    elect_queue = Queue.Queue()
    # Communicate between console and drone on initial master
    init_queue = Queue.Queue()
    # task queue for maple/juice (drone receives task init messages)
    task_queue = Queue.Queue()
    # Initialize membership list
    mlist = member_list()
    # Initilaize Console
    cc = console_client(mlist, host, port, fail_queue, elect_queue, init_queue, task_queue)
    cc.task_queue = task_queue
    cc.start()
    # Initialize Drone
    drn = drone(mlist, host, port,fail_queue,elect_queue,init_queue)
    drn.task_queue = task_queue
    drn.start()
