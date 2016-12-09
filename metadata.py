

# Class stores the metadata information
# Master node uses this class
import socket

class master_metadata:

    def __init__(self,mlist):
        # File_list dictionary
        # key is file_name
        # value is a dictionary of (block_name, VM_list)
        self.file_list = {}
        # block_list dictionary
        # key is VM name
        # value if list of blocks in each VM
        self.block_list = {}
        # size lookup dictionary
        # key is VM name
        # value is number of sdfs blocks in each VM
        self.block_cnt = {}
        self.mlist = mlist
        self.initialize_blocklist()
        
        
        
    # Function for add elements to file_list metadata
    def add_filelist(self, vm_list,sdfsfile):
        (filename,blockid) = sdfsfile.split('_')
        if filename not in self.file_list: 
            block = {}           
        else:
            block = self.file_list[filename]
        if sdfsfile not in block:
            block[sdfsfile] = vm_list
        self.file_list[filename] = block
    
    # Function for initializing block_list   
    def initialize_blocklist(self):
        lst = self.mlist.lst     
        init_lst = []    
        for i in range(len(lst)):
            #Store files in all nodes, except Master
            if (lst[i]['host'] != socket.gethostbyname(socket.gethostname())):
                if lst[i]['host'] not in self.block_list:
                    self.block_list[lst[i]['host']] = init_lst 
                if lst[i]['host'] not in self.block_cnt:   
                    self.block_cnt[lst[i]['host']] = 0 
                
   # Function for updating block_list metadata 
    def add_blocklist(self,vm_list,sdfsfile):
        for key,val in self.block_list.items():            
            new_val = []             
            if key in vm_list:
                if val is None:
                    new_val.append(sdfsfile)
                    self.block_list[key] = new_val 
                else:
                    for x in val:
                        new_val.append(x)
                    new_val.append(sdfsfile)
                    self.block_list[key] = new_val
                self.block_cnt[key] = self.block_cnt[key] + 1 
                del new_val
           
        return 'Successful' 
    
    # Function for deleting entries of a fail node  
    def delete_node(self,fail_node):
        # Get the blocks stored in fail node
        sdfs_lst = self.block_list[fail_node] 
        # Update block_list 
        for key,val in self.block_list.items():   
                if(key == fail_node): 
                  self.block_list.pop(key)
                  self.block_cnt.pop(key)
                  break
        # Update file_list by removing the blocks lost in fail node
        num_replicas = []  
        for each_block in sdfs_lst:
            (file_name,block_id) = each_block.split('_')
            replica_dict = self.file_list[file_name]
            num_replicas.append(len(replica_dict))
            vm_list = replica_dict[each_block]
            vm_list.remove(fail_node)
            replica_dict[each_block] = vm_list 
            self.file_list[file_name] = replica_dict
        return num_replicas
         
    # Function to delete metadata information of a file
    def delete_metadata(self,sdfsfile,block_list):
        # Remove from block_list
        for key,val in self.block_list.items():   
            for block in val:           
              if block in block_list:
                 val.remove(block) 
                 self.block_cnt[key] = self.block_cnt[key] - 1 
                 self.block_list[key] = val
        # Remove from filelist
        if sdfsfile in self.file_list:
            self.file_list.pop(sdfsfile)    
        return 'Successful' 
        
    # Function to delete metadata information of a file
    # that was partially deleted
    # This keeps the sdfs node information and metadata
    # in-sync during unsuccessful delete
    def delete_metadata_partial(self,sdfsfile,block_track):
        # Remove from block_list
        for key,val in self.block_list.items():   
            for block in val:           
              if block in block_track:
                 vm_list = block_track[block]
                 if val in vm_list: 
                     val.remove(block) 
                     self.block_cnt[key] = self.block_cnt[key] - 1 
                     self.block_list[key] = val
        # Remove from filelist
        if sdfsfile in self.file_list:
            replica_dict = self.file_list[sdfsfile] 
            for key,val in replica_dict.items():
                vm_list = block_track[key]
                for each_vm in vm_list:
                    val.remove(each_vm)
                if len(val) > 0:
                    replica_dict[key] = val
                else:
                    replica_dict.pop(key)
            if (len(replica_dict.keys()) > 0) :
                file_list[sdfsfile] = replica_dict  
            else:
                self.file_list.pop(sdfsfile)      
        return 'Successful'     
    
    # Function for updating metadata during fail replication
    # Function for updating metadata after master re-election
    def update_metadata(self,file_name,new_block,rep_node):
        # Increment the block_cnt of the node
        self.block_cnt[rep_node] =  self.block_cnt[rep_node] + 1
        # Update the block_list of VM (rep_node)
        current_val = self.block_list[rep_node]
        #print "Current_val"
        #print type(current_val)     
        #print new_block   
        if current_val is None:
            new_val = []
            new_val.append(new_block)
            self.block_list[rep_node] = new_val
        else:
            current_val.append(new_block)
            self.block_list[rep_node] = current_val
        
        
        # Update file_list meta data 
        if file_name in self.file_list:  
            replica_dict = self.file_list[file_name]
            vm_list = replica_dict[new_block]
            vm_list.append(rep_node)
            replica_dict[new_block] = vm_list
            self.file_list[file_name] = replica_dict 
        else:
            block_dict = {}
            vm_list = []
            vm_list.append(rep_node)
            block_dict[new_block] = vm_list
            self.file_list[file_name] = block_dict

    # Function for printing metadata of Master
    def print_metadata(self):
        print self.block_list
        # print self.block_cnt
        #  print self.file_list  
    
    def return_metadata(self):
        return self.block_list
  

