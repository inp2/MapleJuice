import os
import pickle
import socket
import logging
import subprocess
import getpass
import threading
import sys

# Class in every datanode to handle 
# fail-based replication requests from Master
class node_replicate(threading.Thread):

    def __init__(self):
        super(node_replicate, self).__init__()
        self.dhost = ''
        self.dport = 10025
        self.user = getpass.getuser()        
        
    def create_socket_bind(self):
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            peer_socket.bind((self.dhost, self.dport))    
        except (socket.error,socket.gaierror) as err_msg:
            logging.exception(err_msg)
            peer_socket.close()
            sys.exit()            
        return peer_socket


   # Function to accept connectionfrom Master and replicate blocks
    def recv_cmd(self,peer_socket):
        logging.info("Client Waiting to receive")                        
        while True:   
            peer_socket.listen(10);                
            (conn_socket,(master_ip,master_port)) = peer_socket.accept()
            msg = (conn_socket.recv(8192))
            rcv_msg = pickle.loads(msg)
            # Waits for replicate message
            if (rcv_msg['cmd'] == 'replicate'):
                # Get block to be replicated and remote host-name" 
                block_name = rcv_msg['sdfsblock'] 
                rep_host = rcv_msg['rep_node'] 
                block_ptr = "/usr/local/sdfs_mp3/" + block_name
                scp_done = os.system("scp " + str(block_ptr) + " " + str(self.user) + "@" + str(rep_host) + ":/usr/local/sdfs_mp3/" + str(block_name))
                # Inform Master the success/fail of replication
                if (scp_done == 0):
                    reply_pkt = pickle.dumps({
                'cmd':'rep_done'}) 
                else:
                    reply_pkt = pickle.dumps({
                'cmd':'rep_not_done'}) 
                # Send info to Master
                conn_socket.sendall(reply_pkt)
            conn_socket.close()
            
        peer_socket.close() 
            
    def run(self):
        logging.basicConfig(filename = "node_fail.log", level = logging.INFO, filemode = "w")               
        peer_socket= self.create_socket_bind()
        self.recv_cmd(peer_socket)
        
