import pickle 
import socket
import sys
import logging
from metadata import *
import threading
import getpass
import os

class master_node():

    def __init__(self,mlist,fail_queue):
        #super(master_node, self).__init__()
        # Port of Master
        self.mhost = ''
        self.mport = 10020        
        # Port of client for fail replication
        self.dport = 10025  
        #Master IP
        self.master_id = socket.gethostbyname(socket.gethostname())      
        self.metadata = master_metadata(mlist)
        self.fail_queue = fail_queue
        # Save Master files for delayed processing
        self.master_files = None
        self.mlist = mlist
        # Track number of node_info messages
        self.info = 0
        self.user = getpass.getuser()
        self.task_queue = None
        
    # Function  to create and bind socket
    def create_socket_bind(self):
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            peer_socket.bind((self.mhost, self.mport))    
        except (socket.error,socket.gaierror) as err_msg:
            logging.exception(err_msg)
            peer_socket.close()
            sys.exit()            
        return peer_socket
        
    # Function to accept connection
    # process the command from Datanode/client         
    def recv_cmd(self,peer_socket,lock):
        logging.info("Server Waiting to receive")                        
        while True:   
            peer_socket.listen(15);                
            (conn_socket,(client_ip,client_port)) = peer_socket.accept()
            msg = (conn_socket.recv(8192))
            rcv_msg = pickle.loads(msg)
            lock.acquire()
            # Create entries for new VMs that joined recently
            self.metadata.initialize_blocklist()
            # Receive and handle put command
            if (rcv_msg['cmd'] == 'put'):
                logging.info("Master received Put command")
                vm_list = self.put_handler(rcv_msg['sdfsfile'],rcv_msg['replica_cnt'])
                send_msg = pickle.dumps({'vm_list' : vm_list})
                conn_socket.sendall(send_msg)
                logging.info("Master replied to Put command")
            #Receive and handle get,list and delete
            elif (rcv_msg['cmd'] == 'get' or rcv_msg['cmd'] == 'list' or rcv_msg['cmd'] == 'delete'):
                logging.info("Master received Get/list/delete command")
                file_dict = self.get_handler(rcv_msg['sdfsfile'])
                send_msg = pickle.dumps(file_dict)
                conn_socket.sendall(send_msg)
                logging.info("Master sent file metadata")
            #Receive "delete_done" and update metadata    
            elif (rcv_msg['cmd'] == 'delete_done' ):
                logging.info("Master received Delete_Done command")
                send_msg = self.delete_done_handler(rcv_msg['sdfsfile'],rcv_msg['block_list'])
                conn_socket.sendall(send_msg)
                logging.info("Master Delete Successful")
            #Receive "delete_partial" and update metadata    
            elif (rcv_msg['cmd'] == 'delete_partial' ):
                logging.info("Master received Delete_partial command")
                send_msg = self.delete_partial_handler(rcv_msg['sdfsfile'],rcv_msg['block_track'])
                conn_socket.sendall(send_msg)
                logging.info("Master Delete Partial Successful")
            #Receive "node_info" after new Master is elected
            elif (rcv_msg['cmd'] == 'node_info' ):
                logging.info("New Master received node_info")
                send_msg = self.populate_metadata_handler(client_ip,rcv_msg['sdfslst'])
                conn_socket.sendall(send_msg)
                logging.info("Master node info successful for " + str(client_ip))
            #Receive "put_done" and update metadata    
            elif(rcv_msg['cmd'] == 'put_done'):
                logging.info("Master received put_done")
                send_msg = self.put_done_handler(rcv_msg['sdfsfile'],rcv_msg['vm_list']);
                conn_socket.sendall(send_msg)
                logging.info("Master completed metadata updation")
            # elif(rcv_msg['cmd'] == 'maple'):
            #     logging.info("Master recieved maple command")
            #     self.split_file(rcv_msg['num_maples'], rcv_msg['maple_exe'], rcv_msg['prefix'], rcv_msg['maple_file'])
            conn_socket.close()
            lock.release()
        peer_socket.close() 
            
    def split_file(self, num_maples, maple_exe, sdfs_prefix, maple_file):
        num_lines = sum(1 for line in open(maple_file))
        with open(maple_file) as fh:
            if len(self.mlist.lst) < int(num_maples):
                print "Error: Too Few Machines for Tasks"
                logging.info("Error: Too Few Machines for Tasks")
            tasks = self.mlist.lst[:num_maples]
            for tid in range(num_maples):
                msg = {}
                msg['tid'] = tid
                msg['num_maples'] = num_maples
                msg['maple_exe'] = maple_exe
                msg['prefix'] = sdfs_prefix
                start = tid*num_lines / num_maples
                end = (tid+1)*num_lines / num_maples
                start += 1
                end += 1
                size = end - start
                msg['file_seg'] = ''
                for j in range(size):
                    msg['file_seg'] += fh.readline()
                self.start_task(tid, msg, tasks)
            
    # Start task
    def start_task(self, tid, msg, tasks):
        item = tasks[tid]
        host = item['host']
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node_id = (host, 10044)
            sock.connect(node_id)
            send_pkt = pickle.dumps(msg)
            sock.send(send_pkt)
            sock.close()
        except(socket.error, socket.gaierror) as err_msg:
            logging.info(str(err_msg))

    # Function to update metadata filelist
    # Returns VM's with least number of blocks
    def put_handler(self,sdfsfile,replica_cnt):
        #print self.metadata.print_metadata()
        fname = sdfsfile.split('_')[0]
        if not fname in self.metadata.file_list:
            block_cnt = self.metadata.block_cnt        
            vm_list = []  
            # Get VM's with least number of blocks     
            for key, value in sorted(block_cnt.iteritems(), key=lambda (k,v): (v,k)):            
                vm_list.append(key)
            #print vm_list
            if len(vm_list) >= replica_cnt:           
                return (vm_list[:replica_cnt])
            else:
                return vm_list
        else:
            flist = self.metadata.file_list[fname]
            vm_list = []
            for key in flist:
                vm_list += flist[key]
            return vm_list
    
    
    # Function returns a dictionary.
    # Key is sdfsblockname
    # Val is VM list for every block of the given file
    def get_handler(self,sdfsfile):
        file_dict = {}        
        if sdfsfile in self.metadata.file_list:
            file_dict = self.metadata.file_list[sdfsfile]      
        
        return file_dict            
                
    # Function to update metadata block_list
    # Updated only after client successfully replicates data 
    def put_done_handler(self,sdfsfile,vm_list):
        self.metadata.add_filelist(vm_list,sdfsfile)
        status = self.metadata.add_blocklist(vm_list,sdfsfile)
        #print("Metadata after Put")
        #print self.metadata.print_metadata()
        logging.info("File metadata updated in Master")
        return status
    
    # Function to update metadata block_list
    # Updated only after client successfully deletes replication data 
    def delete_done_handler(self,sdfsfile,block_list):
        status = self.metadata.delete_metadata(sdfsfile,block_list)   
        #print("Metadata after Deletion")     
        #print self.metadata.print_metadata()
        logging.info("File metadata updated in Master")
        return status
    
    # Function to update metadata after partial delete
    # Updated only after client successfully deletes replication data 
    def delete_partial_handler(self,sdfsfile,block_track):
        status = self.metadata.delete_metadata_partial(sdfsfile,block_track)   
        #print("Metadata after Deletion")     
        #print self.metadata.print_metadata()
        logging.info("File metadata updated in Master")
        return status
        
    # Function to handle fail of data nodes and replicate data    
    def datanode_failhdle(self,lock):
        while True:
            # Check if there is a failure detected
            #if not self.fail_queue.empty():
            queue_info = (self.fail_queue.get()).split('/')
            fail_node = queue_info[0]

            '''
            tell maple task watcher that node has failed
            (for task reassignment)
            '''
            task_cmd = {}
            task_cmd['cmd'] = 'fail'
            task_cmd['host'] = fail_node
            self.task_queue.put(task_cmd)

            # If fail_node in tasks list
            # Find another vm and send task
            
            lock.acquire()
            # Get list of blocks stored in fail VM 
            if fail_node in self.metadata.block_list:
                sdfs_lst = self.metadata.block_list[fail_node]
                # Update metadata inform : Delete fail node and update file metadata information
                replica_cnt = self.metadata.delete_node(fail_node)
                # Talk to any other data nodes to create new replicas
                self.replicate_handle(sdfs_lst,fail_node) 
            lock.release()
                
    # Function to talk to other datanodes
    # Send commands to replicate data            
    def replicate_handle(self,sdfs_lst,fail_node):        
        for each_block in sdfs_lst:
            (file_name,block_id) = each_block.split('_')
            replica_dict = self.metadata.file_list[file_name]
            vm_list = replica_dict[each_block] 
            # Try requesting a datanode to replicate data
            # Until replication succeeds            
            for vm in vm_list:
              
               data_host = (str(vm),int(self.dport))
               # Get VM with least number of blocks
               # rep_node = min(self.metadata.block_cnt, 
               # key=self.metadata.block_cnt.get)
               
               vm_min_list = []  
               block_cnt = self.metadata.block_cnt 
               # Get VM's with least number of blocks 
               # and that does not have current replica    
               for key, value in sorted(block_cnt.iteritems(), key=lambda (k,v): (v,k)):    
                   if key not in vm_list:        
                       vm_min_list.append(key)
               #print vm_min_list
               #Get node with minimum number of blocks
               if (len(vm_min_list) > 0):
                   rep_node = vm_min_list[0]
                   send_pkt = pickle.dumps({
                      'cmd':'replicate',
                      'sdfsblock': each_block,
                      'rep_node':rep_node
                  }) 
                   rcv_pkt = self.send_request(send_pkt,data_host)
                   rcv_msg = pickle.loads(rcv_pkt)
                   # Replication successful
                   if (rcv_msg['cmd'] == 'rep_done'):
                   # Update metadata 
                       self.metadata.update_metadata(file_name,each_block,rep_node)
                       break  
                        
    #Function to populate metadata after new master re-election          
    def populate_metadata_handler(self,client_ip, sdfs_lst):  
        self.info = self.info + 1
        size_mlist = len(self.mlist.lst)
        #Populate information from non-master node
        if (self.master_id != client_ip):  
            for each_block in sdfs_lst:
                (file_name,block_id) = each_block.split('_')
                self.metadata.update_metadata(file_name,each_block,client_ip)
        else:
            #Store sdfs information from Master for delayed processing 
            self.master_files = sdfs_lst
            
        #If a node is a Master, move it's files to another datanode
        #This is done only after all data node meta-data is populated
        if(self.info == size_mlist):
            self.info = 0
            for each_block in self.master_files:
                (file_name,block_id) = each_block.split('_')
                replica_dict = self.metadata.file_list[file_name]
                #Get list of current replicas
                vm_list = replica_dict[each_block] 
                vm_min_list = []  
                block_cnt = self.metadata.block_cnt 
                # Get VM's with least number of blocks,that does not have current replica    
                for key, value in sorted(block_cnt.iteritems(), key=lambda (k,v): (v,k)):    
                   if key not in vm_list:        
                       vm_min_list.append(key)   
                #Move Master's copy to datanode with least number 
                # of blocks
                rep_host = vm_min_list[0]
                block_ptr = "/usr/local/sdfs_mp3/" + each_block
                scp_done = os.system("scp " + str(block_ptr) + " " + str(self.user) + "@" + str(rep_host) + ":/usr/local/sdfs_mp3/" + str(each_block))
                #Update metadata after file movement
                self.metadata.update_metadata(file_name,each_block,rep_host)       
            self.master_files = None  
            
        return 'Successful'   
            
            
                 
    # Function establishes socket
    # Sends commands to Client
    # Receives result from Client
    def send_request(self,query, master):
        command = query
        ret = ''
        #print 'datanode address: ' + master(0)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(master)
            send_msg = (command)
            sock.send(send_msg)
            try:
                data = sock.recv(8192)
                ret = ret + data
                
            except socket.timeout:
                logging.info("Socket Timeout in Client")
                # print "Socket TimeOut"
            sock.close()
        except (socket.error,socket.gaierror) as err_msg:
            logging.info(str(err_msg))  
            #print "socket connection error"  
            #print err_msg
         
        return ret 
            
    def run(self):
        logging.basicConfig(filename = "master.log", level = logging.INFO, filemode = "w") 
        lock = threading.Lock()       
        peer_socket= self.create_socket_bind()
        # Start Master thread
        master_thread = threading.Thread(target=self.recv_cmd,args=(peer_socket,lock,))
        # Start failure thread
        failhdle_thread = threading.Thread(target=self.datanode_failhdle,args=(lock,))
        master_thread.daemon = True
        master_thread.start()
        failhdle_thread.daemon = True
        failhdle_thread.start()
        

   
   
	
