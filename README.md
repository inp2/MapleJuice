# MapReduce Framework

The goal is to build a new parallel cloud computing framework called MapleJuice, which bear similarites to MapReduce. MapleJuice shares similarities with MapReduce/Hadoop, except that it is simpler. MapleJuice consists of two phases of computation - Maple (Map) and Juice (Reduce). Each of these phases is parallel, and they are separated by a barrier. MapleJuice is intended to run on an arbitrary number of machines.

## Requirements

python2.7

## Logs

Generated log files include:
* clientdebug.log: Generated from client.py
* intro.log: Generated from intro.py
* serverdebug.log: Generated from server.py
* node.log: Generated from node.py
* faildetector.log: Generated from failure_detector.py
* master.log: Generated from master.py
* node_fail.log: Generated from node_replicate.py

## File List

### Distributed Grep
* client.py: Client code
* server-names.txt: File input with the hostnames of servers
* server.py: Server code

### SimpleDFS
* console.py: Code for each node
* failure_detector.py: Failure Detection code for each node
* intro.py: Code for introducer
* memlist: Membership List
* node.py: Code for node to handle commandline commands
* util.py: Has basic broadcast and unicast functionality
* failure_detector.py: Initialize failure detector class
* master.py: Master Node
* sdfs_utils.py

### MapleJuice

## SimpleDFS Commands

* put <localfilename> <sdfsfilename> - Upload file from local dir
* get <sdfsfilename> ./<localfilename> - Fetches files to local dir
* delete <sdfsfilename - Delete file from all machines
* ls <sdfsfilename> - List all machine addresses where file is stored
* store <sdfsfilename> - List all files stored at this machine
* leader - List current leader

## MapleJuice Commands

### Maple Phase: 
* maple <maple_exe> (wc: word count, rl: reverse web link graph) <maple_file> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>
** <maple_exe> - File name local to the file system
** <sdfs_src_directory> - Specifies the location of the input files

Must partition this entire input data that each of the <num_maples> Maple tasks receives about the same amount of input.

### Juice Phase:
* juice <juice_exe> (wc: word count, rl: reverse web link graph) <juice_file> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}
** <juice_exe> - File name local to the file system of wherever the command is executed
** <num_juices> - Specifies the number of Juice tasks

A user-specified executable that takes as input multiple (key,value) input lines, proesses groups of (key, any_values) input lines together and outputs pairs.

You will have to implement a shuffle grouping mechanism - support both hash and range partitions.

## MapleJuice Code Example

### Start Introducer

`python intro.py - Machine: fa16-cs425-g01-01`

`join`

`elect_leader`

### Start Other Nodes

`python console.py`

`join`

### Run MapleJuice

`maple <maple_exe> (wc: word count, rl: reverse web link graph) <maple_file> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>`

`juice <juice_exe> (wc: word count, rl: reverse web link graph) <juice_file> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}`


## Hadoop/MapleJuice Code Example

## Start Master

VM2: `start-dfs.sh`

Check if namenode/datanode started on VM3, VM4, VM5, VM6: `jps` 

VM2: `start-yarn.sh`

Check if resourcemanager/nodemanager started on VM3, VM4, VM5, VVM6: `jps`

## Place file on VM3 Slave

`hadoop fs -mkdir /user/ipalmer2`

`hadoop fs -mkdir /user/ipalmer2/demo/`

`hadoop fs -mkdir /user/ipalmer2/demo/input`

`hadoop fs -put <file txt> /user/demo/input/<file txt>`

Check file is in HDFS: `hadoop fs –get /user/ipalmer2/demo/input/ <file txt>`

## Run MapReduce (WordCount)

`cd MapReduce_Framework`

Create java executable: `hadoop com.sun.tools.javac.Main WordCount.java`

`jar cf wc.jar WordCount*.class`

`hadoop jar wc.jar WordCount /user/ipalmer2/demo/input/<input file> /user/ipalmer2/out1`

# Get Results

`hadoop fs -get /user/ipalmer2/out1 .`

# Remove file

`hadoop fs -rm -r /user/ipalmer2/out1`


