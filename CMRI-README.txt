This is a version of code that runs in China Mobile Research Institute's clusters and is powered by Apache Hadoop. 

This code is based on Cloudera's Hadoop Distribution (CDH3U4) and Facebook's AvatarNode.

CMRI-CHANGES.txt contains the additional patches that have been committed to the original code base.

For instructions on starting a hadoop cluster, see here: 

System Requirements
	- BCH-1.0.0
	- Zookeeper-3.4.3 or other stable version	
	- At lease 3 nodes. Node1 is primary node, Node2 is standby node, Node3 is NFS server and zookeeper server

0. Compile bc-hadoop
	- download source code of cdh3u4 from Cloudera's website http://archive.cloudera.com/cdh/3/hadoop-0.20.2-cdh3u4.tar.gz
	- copy files/dirs in github to cdh3u4 dir, overwrite once existed.
	- ant package
	
1. Install and Config Zookeeper
	- Please follow the instructions at http://zookeeper.apache.org/
	
2. Install and Config Hadoop
	- Please follow the instructions at http://hadoop.apache.org/ to config HDFS and MapReduce
	
3. Config Hadoop NameNode HA
	- config NFS server on Node3
		. mkdir /nfsSvr
		. edit /etc/exports, add '/nfsSvr *(rw,sync,no_root_squash)'
		. exportfs -rv
		. start nfs service: service nfs start
		. Node1 and Node2 mount this nfs: mount -t nfs Node3:/nfsSvr /mnt/nfs
		. Node1 and Node2 change the privilege of dir /mnt/nfs: chmod 777 /mnt/nfs/
	- add zookeeper and HA jars to hadoop lib dir:
		. cp contrib/highavailability/hadoop-highavailability-0.20.2-cdh3u4.jar lib/
		. cp zookeeper-3.4.3.jar lib/
	- modify the conf files
		. add hostname of Node1 and Node2 to conf/masters
		. vim conf/core-site.xml, add the following contents:
			----------------------------------
			<property>
			  <name>fs.default.name0</name>
			  <value>hdfs://HOSTNAME_Node1:9000</value>
			  <description>The name of the default file system.  A URI whose
			  scheme and authority determine the FileSystem implementation.  The
			  uri's scheme determines the config property (fs.SCHEME.impl) naming
			  the FileSystem implementation class.  The uri's authority is used to
			  determine the host, port, etc. for a filesystem.</description>
			</property>
						
			<property>
			  <name>fs.default.name1</name>
			  <value>hdfs://HOSTNAME_Node2:9000</value>
			  <description>The name of the default file system.  A URI whose
			  scheme and authority determine the FileSystem implementation.  The
			  uri's scheme determines the config property (fs.SCHEME.impl) naming
			  the FileSystem implementation class.  The uri's authority is used to
			  determine the host, port, etc. for a filesystem.</description>
			</property>
			
			<property>
			  <name>fs.ha.zookeeper.quorum</name>
			  <value>IP_OF_Node3</value>
			</property>
			
			<property>
			  <name>fs.ha.zookeeper.prefix</name>
			  <value>/hdfs</value>
			</property>
			
			<property>
			  <name>fs.hdfs.impl</name>
			  <value>org.apache.hadoop.hdfs.DistributedAvatarFileSystem</value>
			  <description>The FileSystem for hdfs: uris.</description>
			</property>
			----------------------------------
			
		. vim conf/hdfs-site.xml, add the following contents:
			----------------------------------
			<property>
			  <name>dfs.http.address0</name>
			  <value>HOSTNAME_Node1:50070</value>
			  <description>
			    The address and the base port where the dfs namenode web ui will listen on.
			    If the port is 0 then the server will start on a free port.
			  </description>
			</property>
			
			<property>
			  <name>dfs.http.address1</name>
			  <value>HOSTNAME_Node2:50070</value>
			  <description>
			    The address and the base port where the dfs namenode web ui will listen on.
			    If the port is 0 then the server will start on a free port.
			  </description>
			</property>
			
			<property>
			  <name>dfs.name.dir.shared0</name>
			  <value>/mnt/nfs/dfs/shared0</value>
			  <description>Determines where on the local filesystem the DFS name node
			      should store the name table(fsimage).  If this is a comma-delimited list
			      of directories then the name table is replicated in all of the
			      directories, for redundancy. </description>
			</property>
			
			<property>
			  <name>dfs.name.dir.shared1</name>
			  <value>/mnt/nfs/dfs/shared1</value>
			  <description>Determines where on the local filesystem the DFS name node
			      should store the name table(fsimage).  If this is a comma-delimited list
			      of directories then the name table is replicated in all of the
			      directories, for redundancy. </description>
			</property>
			
			<property>
			  <name>dfs.name.edits.dir.shared0</name>
			  <value>/mnt/nfs/dfs/edits0</value>
			  <description>Determines where on the local filesystem the DFS name node
			      should store the name table(fsimage).  If this is a comma-delimited list
			      of directories then the name table is replicated in all of the
			      directories, for redundancy. </description>
			</property>
			
			<property>
			  <name>dfs.name.edits.dir.shared1</name>
			  <value>/mnt/nfs/dfs/edits1</value>
			  <description>Determines where on the local filesystem the DFS name node 
			      should store the name table(fsimage).  If this is a comma-delimited list
			      of directories then the name table is replicated in all of the
			      directories, for redundancy. </description>
			</property>
			----------------------------------

4. Start/Stop HDFS with NameNode HA
	- Start the HDFS
		. start zookeeper: bin/zkServer.sh start
		. If the first time to start hdfs, then run the following cmds, else skip this.
			> mkdir -p /mnt/nfs/dfs/edits0  /mnt/nfs/dfs/edits1  /mnt/nfs/dfs/shared0 /mnt/nfs/dfs/shared1
			> bin/hadoop namenode ¨Cformat			
		. bin/start-avatar.sh
	- Stop HDFS
		. bin/stop-avatar.sh
	- Monitor the status of HDFS
		. Web on primary avatarnode: http://Node1:50070/
		. Web on standby avatarnode: http://Node2:50070/
		. Open zookeeper console: bin/zkCli.sh, then check '/activeNameNode' and '/standbyNameNode'
		
5. Config Hadoop JobTracker HA
	- config 	'sudo' to user running hadoop, for example the user 'bigcloud' will start hadoop system.
		. chmod u+w /etc/sudoers
		. edit file /etc/sudoers
			> comment 'Defaults    requiretty' to '#Defaults    requiretty'
			> add bigcloud to sudo: 'bigcloud ALL=NOPASSWD:ALL'			
		. chmod u-w /etc/sudoers
	- add zookeeper jar to hadoop lib: cp zookeeper-3.4.3.jar lib/
	- vim conf/mapred-site.xml, add the following contents
			----------------------------------
				<property>
			  <name>mapred.job.tracker</name>
			  <value>SHARED_VIRTUAL_IP:9001</value>
				</property>
				
				<property>
			  <name>mapred.zookeeper.quorum</name>
			  <value>IP_OF_Node3</value>
				</property>				
				
				<property>
			  <name>mapred.jobtracker.restart.recover</name>
			  <value>true</value>
			  <description>"true" to enable (job) recovery upon restart,
			               "false" to start afresh. default is false, set it true in jobtracker ha.
			  </description>
				</property>
				
				<property>
			  <name>mapred.jobtracker.job.history.block.size</name>
			  <value>524288</value>
			  <description>The block size of the job history file. Since the job recovery
			               uses job history, its important to dump job history to disk as
			               soon as possible. Note that this is an expert level parameter.
			               The default value is set to 3 MB. We set it as 512k in jobtracker ha.
			  </description>
				</property>
			----------------------------------
			SHARED_VIRTUAL_IP is a virtual ip that will be shared among multi jobtrackers, the primary jobtracker will take this virtual ip.

6. Start/Stop MapReduce with JobTracker HA
	- make sure hdfs and zookeeper have be running correctly.
	- start mapreduce
		. make sure the virtual IP is not assigned to any nodes. you can run 'ip addr show|grep eth0' to have a check, or 'sudo -S /sbin/ip addr del SHARED_VIRTUAL_IP/24 dev eth0' to delete the vip assignment.
		. start mapreduce on primary jobtracker : start-mapred.sh
		. start backup jobtracker on other nodes: nohup bin/hadoop jobtracker >>logs/hadoop-bcmr-jobtracker-`hostname`.log &
	- stop mapreduce
		. kill jobtracker processes on backup nodes
		. stop mapreduce on primary jobtracker: stop-mapred.sh
	-  Monitor the status of mapreduce
		. Web on primary jobtracker: http://SHARED_VIRTUAL_IP:50030/
	