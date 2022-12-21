# Simple Distributed File System

SDFS is simple version of HDFS (Hadoop Distributed File System). It is scalable and fault tolerant upto 3 simultaneous failures. 
It uses SWIM style failure detection and ring style leader election algorithm. It is a flat file system, with no concept of directories. 

File operations are: 'put localfilename sdfsfilename' (from local directory), 'get sdfsfilename localfilename' (fetch to local directory), 'delete sdfsfilename', 'ls sdfsfilename' (list the servers where the file and its versions are stored), 'store' (list the files stored at a server), 'get-versions sdfsfilename num-versions localfilename' (fetch the most recent versions of the file into local directory)

## Design

Our distributed file system was built on top of our framework for the MP2 full membership list (i.e. the membership software is the bottom layer of the SDFS stack) and based on a simple implementation of HDFS. At each node, we keep the 4 previous threads (TCP listener, UDP listener, Ping/ACK thread, and command thread), with the addition of one extra thread in the case of the node being elected as the coordinator. All messages flow through a single port, and are sent to the right handler based on type (differentiated between file server, membership info, and coordinator). The introducer is now at a fixed location backed by a file, as per the recommendations. SDFS commands are dispatched to the file server by the command handler thread. SDFS messages are communicated via TCP.
Look at report.pdf for detailed information on design 

# Instructions
- STEP 1: Run the introducer
    * ssh into the machine ```ssh <NETID>@fa22-cs425-ggXX.cs.illinois.edu```
    * Clone the project ```https://gitlab.engr.illinois.edu/sushmam3/mp3_cs425_sdfs.git```
    * Build the project ```mvn -DskipTests package``` (the tests pass, but it's faster to build without them)
    * cd to scripts folder and run the introducer.sh ```./introducer.sh <port-number>```

- STEP 2: Run the member (this will run the file server and leader election as well, one of the members will be chosen as leader)
    * ssh into the machine ```ssh <NETID>@fa22-cs425-ggXX.cs.illinois.edu```
    * Clone the project ```https://gitlab.engr.illinois.edu/sushmam3/mp3_cs425_sdfs.git```
    * Build the project ```mvn -DskipTests package```
    * cd to scripts folder and run the member.sh ```./member.sh <port-number>```
    * On the command prompt, there are 10 options.
        * ```join``` - join the network
        * ```leave``` - leave the network
        * ```list_mem``` - Display the membership list
        * ```list_self``` - Display self information
        * ```put localfilename sdfsfilename``` - Put file into SDFS from local dir. If file exists it will create a new version.
        * ```get sdfsfilename localfilename``` - Get file from SDFS into local dir
        * ```delete sdfsfilename``` - Delete the file from SDFS
        * ```ls sdfsfilename``` - list all machine (VM) addresses where this file is currently
          being stored
        * ```store``` - At any machine, list all files currently being stored at this
          machine
        * ```get-versions sdfsfilename num-versions localfilename``` - gets all the last num-versions
          versions of the file into the localfilename (uses delimiters to mark out
          versions). 
