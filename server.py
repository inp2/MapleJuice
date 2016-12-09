import socket
import sys
import logging
import subprocess

# Function  to create and bind socket
def create_socket_bind(host,port):
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            peer_socket.bind((host, port))    
        except (socket.error,socket.gaierror) as err_msg:
            logging.exception(err_msg)
            peer_socket.close()
            sys.exit()
        peer_socket.listen(10);    
        return peer_socket
        
        

# Function to accept connection, send and receive data         
def send_recv_data(peer_socket,bytes_to_receive):
        logging.info("Server Waiting to receive")
                      
        while True:   
            (conn_socket,conn_address) = peer_socket.accept()
            rcvd_msg = (conn_socket.recv(bytes_to_receive))
            decoded_msg = ''
            if rcvd_msg.strip():
                logging.info("Received valid message")
                decoded_msg = decoded_msg + rcvd_msg
                logging.info("Unix command: " + decoded_msg)
                try:
                    cmd_outp = subprocess.check_output(decoded_msg,shell=True)
                    cmd_outp = cmd_outp + 'CMD_END'
                    logging.info("Sending Output of CMD\n")
                    conn_socket.sendall(cmd_outp)
                    logging.info("Finished Sending Result\n")
                except subprocess.CalledProcessError as err_msg:
                    logging.error("Command Failed to execute")
                    logging.exception(err_msg)
                    conn_socket.sendall('CMD_FAIL_FROM_REMOTE'+'CMD_END')
                        
                
        logging.info("Server loop exited")    
        conn_socket.close()
        peer_socket.close() 

# Main Function to connect and start logging             
if __name__ == "__main__":        
    host = ''
    port = 10005
    bytes_to_receive = 8192             
    logging.basicConfig(filename = "serverdebug.log", level = logging.INFO, filemode = "w")
    peer_socket= create_socket_bind(host, port)
    send_recv_data(peer_socket,bytes_to_receive)
    sys.exit()
