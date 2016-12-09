import os
import pickle
import socket
import logging
import subprocess
# Class with helper functions for Handling SDFS Operations 
# Communicates with Master for all types of queries
# Splits file into blocks during put operation
# Retrieves metadata of file for get, delete and list operations
# Joins the SDFS blocks into single file during get operation
class sdfs_utils:
    def __init__(self):
        pass
        
    # Function to handle put operation
    # Contacts master for each block of file
    # Transfers the file blocks to replica nodes 
    # Function to handle put operation
    # Contacts master for each block of file
    # Transfers the file blocks to replica nodes 
    def put_handler(self,lfile,sfile,bsize,rep,master,user):
        
        num_blocks = self.split_file(lfile,sfile,bsize)
        (dir_name,file_name) = os.path.split(lfile)
        for block_id in range(num_blocks):
            rep_list = []
            block_name = sfile + '_block' + str(block_id+1)
            block_ptr = os.path.join(dir_name,block_name)
            pkt = pickle.dumps({
                'cmd':'put',
                'sdfsfile': block_name,
                'replica_cnt': rep
            }) 
            # print "Receiving querying back from master"
            # Contact the Master and get list of VM's for this block   
            rcv_pkt = self.send_query(pkt,master)
            # print rcv_pkt
            rcv_msg = pickle.loads(rcv_pkt)
            rep_list = rcv_msg['vm_list']
            # print "Replica_nodes " 
            # print rep_list  
            
            if len(rep_list) < rep:
                #3 print "Not enough replica nodes exists" 
                break
            # Copy file to replica node through SCP
            # Abort copy if any of SCP fails
            for item in rep_list: 
                replica_fail = 0      
                cp_command = "ssh " + str(user) + "@" + str(item) + ' "cat >> /usr/local/sdfs_mp3/' + str(block_name) + '" < ' + str(block_ptr)
                # print cp_command
                scp_done = os.system(cp_command)
                # scp_done = os.system("scp " + str(block_ptr) + " " + str(user) + "@" + str(item) + ":/usr/local/sdfs_mp3/" + str(block_name))  
                if (scp_done!=0):
                    print "SCP on remote host failed"
                    replica_fail = 1
                    break  
                if(replica_fail == 1):
                    print "Replication Failed, re-try put command" 
                    break
            else:
                # If all replication is successful, send put_done to Master
                # Master updates its metadata on receiving put_done
                logging.info("Put Complete: " + str(block_name))
                pkt = pickle.dumps({
                'cmd':'put_done',
                'sdfsfile': block_name,
                'vm_list': rep_list
            })  
                result_metadata = self.send_query(pkt,master)
                # print "Done Done"
                # logging.info("Metadata Update is : " + str(result_metadata))
                # print "Metadata Update at Master is " + str(result_metadata)
        # Garbage collection at local directory: Remove all temporary split files 
        
        ##rpn2 TBD
        #CMD = "rm " + str(block_ptr) 
        #print CMD
        #subprocess.call(str(CMD), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
            
    # Function to split the file into blocks
    # Return overall number of blocks
    # The split files are stored in the same folder as the input file
    def split_file(self,lfile,sfile,bsize):   
        logging.info("File Split Begin");        
        chunk_size = 1024*1024*bsize        
        lfile_ptr = open(lfile, 'rb');
        block_id = 1;
        # print "In Split File"
        (dir_name,file_name) = os.path.split(lfile)        
        while True:
            block_content = lfile_ptr.readlines(chunk_size)
            if not block_content: 
                break
            block_name = sfile + '_block' + str(block_id)            
            block_ptr = os.path.join(dir_name,block_name)            
            sfile_ptr = open(block_ptr,'wb')
            sfile_ptr.writelines(block_content)
            block_id = block_id + 1
            # print "File File File"
            sfile_ptr.close()       
        lfile_ptr.close() 
        logging.info("File Split Complete");            
        return (block_id-1)
        
        
    # Function to get metadata of a given file
    # Metadata includes node names and block names
    def get_metadata(self,sfile,master):
        pkt = pickle.dumps({
                'cmd':'list',
                'sdfsfile': sfile
            }) 
        rcv_pkt = self.send_query(pkt,master)
        rcv_msg = pickle.loads(rcv_pkt)
        # print "Within List Handler"
        # print rcv_msg  
        return rcv_msg  
        
    def get_handler(self,lfile,sfile,master,user):
        # Get file metadata info from Master
        file_mdata = self.get_metadata(sfile,master)
        # Get individual blocks from the first datanode in value_list
        # key is block_name and value is list of VM's where a block is stored
        num_blocks = len(file_mdata)
        (dir_name,file_name) = os.path.split(lfile)
        block_copy = 0
        if (num_blocks > 0):
            for key,val in file_mdata.items():
                block_copy = 0
                for host in val:
                    scp_done = os.system("scp " + str(user) + "@" + str(host) + ":/usr/local/sdfs_mp3/" + str(key) + " " + str(dir_name) + "/" )
                    if (scp_done ==0):
                        # print 'Atleast one successful copy' 
                        block_copy = 1
                        break  
                if(block_copy == 0):
                    # print "Cannot retrieve " + str(key) + ", Copy Failed"
                    # print "Retry Get Command"
                    break
        else:
            print "Metadata is empty, trying to get non-existing file"         
        # Execute Join only if all blocks have been retrieved       
        if (block_copy == 1):
            # Join individual blocks 
            lfile_ptr = open(lfile, 'ab');
            for block_id in range(num_blocks):
                block_name = sfile + '_block' + str(block_id+1)
                block_ptr = os.path.join(dir_name,block_name)
                sfile_ptr = open(block_ptr,'rb')
                lfile_ptr.writelines(sfile_ptr.readlines())
                sfile_ptr.close() 
            lfile_ptr.close()
            # print 'Combined blocks'
        #rpn2 : TBD
        #Garbage collection at local directory: Remove all temporary block files
        #print str(dir_name)
        #subprocess.call(["rm ", str(dir_name) + "/*block*"], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)        
    
    # Function establishes socket
    # Sends commands to Master and receives result from Master
    def send_query(self,query, master):
        command = query
        ret = ''
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(master)
            send_msg = (command)
            sock.sendall(send_msg)
            try:
                data = sock.recv(8192)
                ret = ret + data
                
            except socket.timeout:
                logging.info("Socket Timeout in Client")
                # print "Socket TimeOut"
            sock.close()
        except (socket.error,socket.gaierror) as err_msg:
            logging.info(str(err_msg))    
            # print err_msg
         
        return ret
    
    def delete_handler(self,sfle,master,user):
        
        # Get file metadata info from Master
        file_mdata = self.get_metadata(sfle,master)
        
        #To track unsuccessful deletes
        block_track = {}
        del_done = 1
        # Set remove command; Remove all replicas for all blocks
        for key,val in file_mdata.items():
            block_track[key] = None
            vm_track = []
            for each_vm in val:
                del_done = 1                
                CMD="rm /usr/local/sdfs_mp3/" + str(key)      
                # Remove file from this location
                ret = subprocess.call(["ssh", str(user) + "@" + str(each_vm), CMD], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if (ret !=0):
                    del_done = 0;
                    print 'Unsuccessful delete'
                    break
                vm_track.append(each_vm)
                block_track[key] = vm_track 
            if(del_done ==0):
                break 
        # If all elements are deleted successfuly
        # Inform master to update metadata
        if(del_done == 1):   
            pkt = pickle.dumps({
                    'cmd':'delete_done',
                    'sdfsfile':sfle,
                    'block_list': file_mdata.keys()
                }) 
            result = self.send_query(pkt,master) 
            # print "Metadata Updation after Delete is:" + str(result)      
            logging.info("Delete Complete: " + str(sfle))
        # Inform master to update metadata for partial delete
        # This ensures node information and metadata are in sync
        else:           
           # print "Delete unsuccessful, re-try delete cmd"
           pkt = pickle.dumps({
                    'cmd':'delete_partial',
                    'sdfsfile':sfle,
                    'block_track': block_track
                }) 
           result = self.send_query(pkt,master) 
           # print "Metadata Updation after Partial Delete is:" + str(result)      
           logging.info("Delete Partial: " + str(sfle)) 
    
    #Contact Master and provide node information 
    #Called after new master is re-elected
    def node_info_handler(self,file_list,master):
        pkt = pickle.dumps({
                'cmd':'node_info',
                'sdfslst': file_list
            }) 
        rcv_pkt = self.send_query(pkt,master)
        #rcv_msg = pickle.loads(rcv_pkt)
