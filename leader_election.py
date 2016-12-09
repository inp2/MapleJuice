import pickle 
import socket
import sys
import logging
import threading
import time
import random
import Queue


class LeaderElection():

    def __init__(self,mlist,elect_queue):
        #super(master_node, self).__init__()
        # Election messages are sent over below port
        self.ehost = ''
        self.eport = 10040 
        #Reference membership list
        self.mlist = mlist            
        self.current_node = socket.gethostbyname(socket.gethostname())
        #Elect Queue to send election result to other threads
        self.elect_queue = elect_queue
        #Use election_id to eliminate recurring election runs
        self.election_id = None
        #Node_id is last octect of IPv4 address
        ip_split = self.current_node.split(".")
        self.current_id = int(ip_split[len(ip_split)-1])
        # To check if election is in progress
        self.election_in_progress = False
        #To check for election ack
        self.election_ack = False
        #To check for co-ordination message
        self.rcvd_coordination = False
        #Track current leader
        self.leader = None
        self.election_hdle = None
        
        
        
    # Function  to create and bind socket
    def create_socket_bind(self):
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            peer_socket.bind((self.ehost, self.eport))    
        except (socket.error,socket.gaierror) as err_msg:
            logging.exception(err_msg)
            peer_socket.close()
            sys.exit()            
        return peer_socket
        
        

    # Function to accept connection, process the command from Datanode/client         
    def recv_cmd(self,peer_socket,lock):
        logging.info("Election thread Waiting to receive")                        
        while True:   
            peer_socket.listen(20);                
            (conn_socket,(client_ip,client_port)) = peer_socket.accept()
            msg = (conn_socket.recv(8192))
            rcv_msg = pickle.loads(msg)
            
            #Handle initial leader election
            #Initiate new leader when master node fail is detected
            if(rcv_msg['cmd'] == 'initiate'):
                logging.info("Received Fail detect/initiate message")
                
                if(self.election_id != rcv_msg['id']):                      
                    self.current_state = 'election' 
                    self.election_id = rcv_msg['id'] 
                    
                    if (self.election_in_progress == False):                        
                        self.election_in_progress = True
                        self.election_ack = False
                        self.rcvd_coordination = False                        
                        self.election_hdle = threading.Thread(target=self.election_msg_hdler(lock,))
                        self.election_hdle.daemon = True
                        self.election_hdle.start()
                    
                    conn_socket.sendall("Initiated")
                    
            #Receive and handle election message
            elif (rcv_msg['cmd'] == 'election'):
                logging.info("Received Election message ")
                ip_split = client_ip.split(".")
                client_id = int(ip_split[len(ip_split)-1])
                #Send Ack to received election message
                send_pkt = pickle.dumps({
                    'cmd':'election_ack'
                    }) 
                self.send_query_master(send_pkt,(client_ip,int(self.eport)))
                logging.info("Sent Ack to election initiator")
                # print "Sent Ack to election initiator " + str(client_ip)   
                #Start a new election run if new election has not begun yet 
                if (self.election_id != rcv_msg['id']): 
                    self.election_id = rcv_msg['id'] 
                    
                    if (self.election_in_progress == False):
                        self.election_in_progress = True                        
                        self.election_ack = False
                        self.rcvd_coordination = False                        
                        self.election_hdle = threading.Thread(target=self.election_msg_hdler(lock,))
                        self.election_hdle.daemon = True
                        self.election_hdle.start()
                else:
                    logging.info("Duplicate Election Message, No new election run taken")
                        
            #Receive and handle election_ack message
            elif (rcv_msg['cmd'] == 'election_ack'):
                logging.info("Received Election Ack message ") 
                self.election_ack = True
                # print "Receive Ack from " + str(client_ip)
                
            #Receive and handle coordinate message
            elif (rcv_msg['cmd'] == 'coordinate'):
                logging.info("Received cordinate message ")
                
                # print "Received cordinate message from" + str(client_ip)
                
                if (self.rcvd_coordination == False):
                    #New leader is communicated
                    
                    self.elect_queue.put(client_ip)
                    self.leader = client_ip
                    self.rcvd_coordination = True
                    self.election_ack = True
                    self.election_in_progress = False
                    if self.election_hdle is not None:
                        self.election_hdle.join()
                        self.election_hdle = None
            conn_socket.close()
        peer_socket.close() 
            
    def election_msg_hdler(self,lock):
    
        #ID is defined as last octet of IPv4 address
        ip_split = self.current_node.split(".")
        current_id = int(ip_split[len(ip_split)-1]);
        lst = self.mlist.lst
        
        #List of higher ID VM's
        vm_list = []
        
        #Find VM's with higher ID
        for i in range(len(lst)): 
            host = lst[i]['host']
            ip_split = host.split(".")
            host_id =  int(ip_split[len(ip_split)-1]);
            if (host_id > current_id):
                vm_list.append(host)
                
        #Send Election message to higher ID VM's 
        send_pkt = pickle.dumps({
                    'cmd':'election',
                    'id' : self.election_id
                    }) 
                  
        if len(vm_list) > 0:
            for each_vm in vm_list:
                # print "X.Send Election Message to" + str(each_vm)
                node_id = (each_vm, int(self.eport)) 
                self.send_query(send_pkt,node_id) 
            #Wait for election acknowledge           
            self.election_ack_wait(lock)
            
        #No higher ID node's exist, current node is leader
        #Send co-ordination message to all nodes
        else:
            
            send_pkt = pickle.dumps({
                    'cmd':'coordinate',
                    'id' : self.election_id
                    }) 
            for i in range(len(lst)): 
                host = lst[i]['host']
                # print "1.Send Coordination Message"                
                node_id = (host, int(self.eport))
                self.send_query(send_pkt,node_id)
            
    #Wait for AcK message from any one higher order node
    def election_ack_wait(self,lock): 
    
        size_mlist = len(self.mlist.lst)
        
        # Set wait timeout to be function of node_id, lower node_id's wait longer than higher node_ids
        # Fix for thread not yielding
        mult = 16 - int(self.current_id)
        if mult > 0:
            t1 = mult
        else:
            t1 = 5
        if size_mlist > 0:            
            wait_timeout =  (2.8/(2*size_mlist - 1)) *  t1
        else:            
            wait_timeout = (0.120) * t1 
            
        # Sleep, wait for ack
        # print "Wait for ack timeout"
        time.sleep(wait_timeout)
        
        lock.acquire()
        ack_rcvd = self.election_ack
        crd_rcvd = self.rcvd_coordination
        lock.release()
        
            
        #If received ACK, wait for co-ordination
        if (ack_rcvd == True and crd_rcvd == False):                     
            #wait to receive leader info
            self.coordination_wait(wait_timeout,lock)   
        #Current Node is leader, no ACK received after timeout 
        elif (ack_rcvd == False and crd_rcvd == False):    
            send_pkt = pickle.dumps({
                    'cmd':'coordinate',
                    'id' : self.election_id
                    }) 
            lst = self.mlist.lst
            #Send coordinate messages to all nodes
            for i in range(len(lst)): 
                host = lst[i]['host']
                
                # print "2.Send Coordination Message"
                node_id = (host, int(self.eport))
                self.send_query(send_pkt,node_id)
        else:
            #end of thread
            pass
                    
    #wait to receive leader info
    def coordination_wait(self, wait_timeout,lock):
          #Co-ordination timeout to be 3 times the wait_ack timeout
          final_timeout = wait_timeout * 3.0 
          #Sleep for timeout
          #print "Wait for co-ordination timeout"
          time.sleep(final_timeout) 
          
          # If no coordinator message has arrived, 
          # initiate another election
          # Send election message to itself
          lock.acquire()          
          crd_rcvd = self.rcvd_coordination
          lock.release()
          
          if(crd_rcvd == False):
              send_pkt = pickle.dumps({
                      'cmd':'election',
                      'id' :self.current_node
                      }) 
              node_id = ('', int(self.eport))
              # print "Re-election"              
              lock.acquire() 
              self.election_in_progress = False
              lock.release()   
              self.send_query(send_pkt,node_id)
          
                  
    # Function establishes socket
    # Sends commands to other nodes in election process
    def send_query(self,query, master):
        command = query
        ret = ''
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(master)
            send_msg = (command)
            sock.sendall(send_msg)
            sock.close()
        except (socket.error,socket.gaierror) as err_msg:
            logging.info(str(err_msg))    
            # print err_msg         
        return ret  
    
    # Function establishes socket
    # Sends commands to other nodes in election process
    def send_query_master(self,query, master):
        command = query
        ret = ''
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(master)
            send_msg = (command)
            sock.sendall(send_msg)
            sock.close()
        except (socket.error,socket.gaierror) as err_msg:
            logging.info(str(err_msg))    
            # print err_msg         
        return ret  
              
    def run(self):
        logging.basicConfig(filename = "leader.log", level = logging.INFO, filemode = "w") 
        lock = threading.Lock() 
        peer_socket= self.create_socket_bind()
        # Start election messaging thread
        election_thread = threading.Thread(target=self.recv_cmd,args=(peer_socket,lock,))        
        election_thread.daemon = True
        election_thread.start()

        
        

   
   
	


