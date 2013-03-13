/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.AvatarZooKeeperClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HaMonitor;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.protocol.UnregisteredDatanodeException;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.PluginDispatcher;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.SingleArgumentRunnable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;


/**
 * This is an implementation of the AvatarDataNode, a wrapper
 * for a regular datanode that works with AvatarNode.
 * 
 * The AvatarDataNode is needed to make a vanilla DataNode send
 * block reports to Primary and standby namenodes. The AvatarDataNode
 * does not know which one of the namenodes is primary and which is
 * secondary.
 *
 * Typically, an adminstrator will have to specify the pair of
 * AvatarNodes via fs1.default.name and fs2.default.name
 *
 */

public class AvatarDataNode extends DataNode {

  static {
    Configuration.addDefaultResource("avatar-default.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }
  public static final Log LOG = LogFactory.getLog(AvatarDataNode.class.getName());

  volatile boolean shutdown = false;

  //add by guoleitao
  InetSocketAddress nameAddr1;
  InetSocketAddress nameAddr2;
  DatanodeProtocol namenode1;
  DatanodeProtocol namenode2;
  AvatarProtocol avatarnode1;
  AvatarProtocol avatarnode2;
  InetSocketAddress avatarAddr1;
  InetSocketAddress avatarAddr2;
  boolean doneRegister1 = false;    // not yet registered with namenode1
  boolean doneRegister2 = false;    // not yet registered with namenode2
  OfferService offerService1;
  OfferService offerService2;
  volatile OfferService primaryOfferService = null;
  Thread of1;
  Thread of2;
  Method transferBlockMethod; 
  AvatarZooKeeperClient zkClient = null;
  
  NNMonitor nnm = null;
  Daemon nnmThread = null; //Thread to monitor the HA failover.
//end add guoleitao 

private long readaheadLength;

private boolean dropCacheBehindWrites;

private boolean syncBehindWrites;

private boolean dropCacheBehindReads;
  
  public AvatarDataNode(Configuration conf, AbstractList<File> dataDirs, 
                        String dnThreadName) throws IOException {
    super(conf, dataDirs);    
    AvatarDataNode.dnThreadName = dnThreadName;
    
    //add by guoleitao
    // access a private member of the base DataNode class
    try { 
      Method[] methods = DataNode.class.getDeclaredMethods();
      for (int i = 0; i < methods.length; i++) {
        if (methods[i].getName().equals("transferBlock")) {
          transferBlockMethod = methods[i];
        }
      }
      if (transferBlockMethod == null) {
        throw new IOException("Unable to find method DataNode.transferBlock.");
      }
      transferBlockMethod.setAccessible(true);
    } catch (java.lang.SecurityException exp) {
      throw new IOException(exp);
    }
    //end add guoleitao
  }
  
  /**
   * Returns list of InetSocketAddresses corresponding to namenodes from the
   * configuration. 
   * @param suffix 0 or 1 indicating if this is AN0 or AN1
   * @param conf configuration
   * @param keys Set of keys
   * @return list of InetSocketAddress
   * @throws IOException on error
   */
//  private static List<InetSocketAddress> getRPCAddresses(String suffix,
//      Configuration conf, Collection<String> serviceIds, String... keys) throws IOException {
//    // Use default address as fall back
//    String defaultAddress;
//    try {
//      defaultAddress = conf.get(FileSystem.FS_DEFAULT_NAME_KEY + suffix);
//      if (defaultAddress != null) {
//        Configuration newConf = new Configuration(conf);
//        newConf.set(FileSystem.FS_DEFAULT_NAME_KEY, defaultAddress);
//        defaultAddress = NameNode.getHostPortString(NameNode.getAddress(newConf));
//      }
//    } catch (IllegalArgumentException e) {
//      defaultAddress = null;
//    }
//    
//    for (int i = 0; i < keys.length; i++) {
//      keys[i] += suffix;
//    }
//    
//    List<InetSocketAddress> addressList = DFSUtil.getAddresses(conf,
//        serviceIds, defaultAddress,
//        keys);
//    if (addressList == null) {
//      String keyStr = "";
//      for (String key: keys) {
//        keyStr += key + " ";
//      }
//      throw new IOException("Incorrect configuration: namenode address "
//          + keyStr
//          + " is not configured.");
//    }
//    return addressList;
//  }
//  
//  private static List<InetSocketAddress> getDatanodeProtocolAddresses(
//      Configuration conf, Collection<String> serviceIds) throws IOException {
//    // Use default address as fall back
//    String defaultAddress;
//    try {
//      defaultAddress = conf.get(FileSystem.FS_DEFAULT_NAME_KEY);
//      if (defaultAddress != null) {
//        Configuration newConf = new Configuration(conf);
//        newConf.set(FileSystem.FS_DEFAULT_NAME_KEY, defaultAddress);
//        defaultAddress = NameNode.getHostPortString(NameNode.getAddress(newConf));
//      }
//    } catch (IllegalArgumentException e) {
//      defaultAddress = null;
//    }
//    
//    List<InetSocketAddress> addressList = DFSUtil.getAddresses(conf,
//        serviceIds, defaultAddress,
//        NameNode.DATANODE_PROTOCOL_ADDRESS,
//        FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);
//    if (addressList == null) {
//      throw new IOException("Incorrect configuration: namenode address "
//          + FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY
//          + " is not configured.");
//    }
//    return addressList;
//  }

  @Override
  void startDataNode(Configuration conf, 
                     AbstractList<File> dataDirs
                     , SecureResources resources ) throws IOException {
	//SecureResources is not used in this function. add by guoleitao
//    initGlobalSetting(conf, dataDirs);
//    
//    Collection<String> serviceIds = DFSUtil.getNameServiceIds(conf);
//    List<InetSocketAddress> defaultNameAddrs =
//      AvatarDataNode.getDatanodeProtocolAddresses(conf, serviceIds);
//    List<InetSocketAddress> nameAddrs0 = 
//      AvatarDataNode.getRPCAddresses("0", conf, serviceIds,
//           NameNode.DATANODE_PROTOCOL_ADDRESS, FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);
//    List<InetSocketAddress> nameAddrs1 = 
//      AvatarDataNode.getRPCAddresses("1", conf, serviceIds,
//           NameNode.DATANODE_PROTOCOL_ADDRESS, FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);
//    List<InetSocketAddress> avatarAddrs0 =
//      AvatarDataNode.getAvatarNodeAddresses("0", conf, serviceIds);
//    List<InetSocketAddress> avatarAddrs1 =
//      AvatarDataNode.getAvatarNodeAddresses("1", conf, serviceIds);
//
//    namespaceManager = new AvatarNamespaceManager(nameAddrs0, nameAddrs1,
//        avatarAddrs0, avatarAddrs1, defaultNameAddrs, 
//        DFSUtil.getNameServiceIds(conf));
//
//    initDataSetAndScanner(conf, dataDirs, nameAddrs0.size());
	  
	  // the following added by guoleitao
		if (UserGroupInformation.isSecurityEnabled() && resources == null
				&& conf.getBoolean("dfs.datanode.require.secure.ports", true))
			throw new RuntimeException(
					"Cannot start secure cluster without privileged resources. "
							+ "In a secure cluster, the DataNode must be started from within "
							+ "jsvc. If using Cloudera packages, please install the "
							+ "hadoop-0.20-sbin package.\n\n"
							+ "For development purposes ONLY you may override this check by setting"
							+ " dfs.datanode.require.secure.ports to false. *** THIS WILL OPEN A "
							+ "SECURITY HOLE AND MUST NOT BE USED FOR A REAL CLUSTER ***.");
		 this.secureResources = resources;
		 
	   // use configured nameserver & interface to get local hostname
	    if (conf.get("slave.host.name") != null) {
	      machineName = conf.get("slave.host.name");   
	    }
	    if (machineName == null) {
	      machineName = DNS.getDefaultHost(
	                                     conf.get("dfs.datanode.dns.interface","default"),
	                                     conf.get("dfs.datanode.dns.nameserver","default"));
	    }
	    
	    InetSocketAddress nameNodeAddr = NameNode.getServiceAddress(conf, true);
	    
	    this.socketTimeout =  conf.getInt("dfs.socket.timeout",
	                                      HdfsConstants.READ_TIMEOUT);
	    this.socketWriteTimeout = conf.getInt("dfs.datanode.socket.write.timeout",
	                                          HdfsConstants.WRITE_TIMEOUT);
	    /* Based on results on different platforms, we might need set the default 
	     * to false on some of them. */
	    this.transferToAllowed = conf.getBoolean("dfs.datanode.transferTo.allowed", 
	                                             true);
	    this.writePacketSize = conf.getInt("dfs.write.packet.size", 64*1024);
	    this.readaheadLength = conf.getLong(
	            DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_KEY,
	            DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
	    this.dropCacheBehindWrites = conf.getBoolean(
	            DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_KEY,
	            DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_DEFAULT);
	    this.syncBehindWrites = conf.getBoolean(
	            DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_KEY,
	            DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_DEFAULT);
	    this.dropCacheBehindReads = conf.getBoolean(
	            DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_READS_KEY,
	            DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_READS_DEFAULT);
		this.relaxedVersionCheck = conf.getBoolean(
				CommonConfigurationKeys.HADOOP_RELAXED_VERSION_CHECK_KEY,
				CommonConfigurationKeys.HADOOP_RELAXED_VERSION_CHECK_DEFAULT);
	    
	    InetSocketAddress socAddr = DataNode.getStreamingAddr(conf);
//	    String address = 
//	      NetUtils.getServerAddress(conf,
//	                                "dfs.datanode.bindAddress", 
//	                                "dfs.datanode.port",
//	                                "dfs.datanode.address");
//	    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
	    int tmpPort = socAddr.getPort();
	    storage = new DataStorage();
	    // construct registration
	    this.dnRegistration = new DatanodeRegistration(machineName + ":" + tmpPort);

	    this.namenode = new DatanodeProtocols(2); // override DataNode.namenode
	    nameAddr1 = AvatarDataNode.getNameNodeAddress(getConf(), "fs.default.name0", "dfs.namenode.dn-address0");
	    nameAddr2 = AvatarDataNode.getNameNodeAddress(getConf(), "fs.default.name1", "dfs.namenode.dn-address1");
	    avatarAddr1 = AvatarDataNode.getAvatarNodeAddress(getConf(), "fs.default.name0");
	    avatarAddr2 = AvatarDataNode.getAvatarNodeAddress(getConf(), "fs.default.name1");
	    zkClient = new AvatarZooKeeperClient(getConf(), null);

	    // get version and id info from the name-node
	    NamespaceInfo nsInfo = handshake(true);
	    StartupOption startOpt = getStartupOption(conf);
	    assert startOpt != null : "Startup option must be set.";
	    
	    boolean simulatedFSDataset = 
	        conf.getBoolean("dfs.datanode.simulateddatastorage", false);
	    if (simulatedFSDataset) {
	        setNewStorageID(dnRegistration);
	        dnRegistration.storageInfo.layoutVersion = FSConstants.LAYOUT_VERSION;
	        dnRegistration.storageInfo.namespaceID = nsInfo.namespaceID;
	        // it would have been better to pass storage as a parameter to
	        // constructor below - need to augment ReflectionUtils used below.
	        conf.set("StorageId", dnRegistration.getStorageID());
	        try {
	          //Equivalent of following (can't do because Simulated is in test dir)
	          //  this.data = new SimulatedFSDataset(conf);
	          this.data = (FSDatasetInterface) ReflectionUtils.newInstance(
	              Class.forName("org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset"), conf);
	        } catch (ClassNotFoundException e) {
	          throw new IOException(StringUtils.stringifyException(e));
	        }
	    } else { // real storage
	      // read storage info, lock data dirs and transition fs state if necessary
	      storage.recoverTransitionRead(nsInfo, dataDirs, startOpt);
	      // adjust
	      this.dnRegistration.setStorageInfo(storage);
	      // initialize data node internal structure
	      this.data = new FSDataset(storage, conf);
	    }

	    // Allow configuration to delay block reports to find bugs
	    artificialBlockReceivedDelay = conf.getInt(
	      "dfs.datanode.artificialBlockReceivedDelay", 0);

	    // register datanode MXBean
	    this.registerMXBean(conf); // register the MXBean for DataNode
	    
	    
	    // find free port or use privileged port provide
	    ServerSocket ss;
	    if(secureResources == null) {
	      ss = (socketWriteTimeout > 0) ? 
	        ServerSocketChannel.open().socket() : new ServerSocket();
	      Server.bind(ss, socAddr, 0);
	    } else {
	      ss = resources.getStreamingSocket();
	    }	    
	    ss.setReceiveBufferSize(DEFAULT_DATA_SOCKET_SIZE); 
	    // adjust machine name with the actual port
	    tmpPort = ss.getLocalPort();
	    selfAddr = new InetSocketAddress(ss.getInetAddress().getHostAddress(),
	                                     tmpPort);
	    this.dnRegistration.setName(machineName + ":" + tmpPort);
	    LOG.info("Opened info server at " + tmpPort);
	      
	    this.threadGroup = new ThreadGroup("dataXceiverServer");
	    this.dataXceiverServer = new Daemon(threadGroup, 
	        new DataXceiverServer(ss, conf, this));
	    this.threadGroup.setDaemon(true); // auto destroy when empty

	    this.blockReportInterval =
	      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
	    this.initialBlockReportDelay = conf.getLong("dfs.blockreport.initialDelay",
	                                            BLOCKREPORT_INITIAL_DELAY)* 1000L; 
	    if (this.initialBlockReportDelay >= blockReportInterval) {
	      this.initialBlockReportDelay = 0;
	      LOG.info("dfs.blockreport.initialDelay is greater than " +
	        "dfs.blockreport.intervalMsec." + " Setting initial delay to 0 msec:");
	    }
	    this.heartBeatInterval = conf.getLong("dfs.heartbeat.interval", HEARTBEAT_INTERVAL) * 1000L;
//	    DataNode.nameNodeAddr = nameNodeAddr;

	    //initialize periodic block scanner
	    String reason = null;
	    if (conf.getInt("dfs.datanode.scan.period.hours", 0) < 0) {
	      reason = "verification is turned off by configuration";
	    } else if ( !(data instanceof FSDataset) ) {
	      reason = "verifcation is supported only with FSDataset";
	    } 
	    if ( reason == null ) {
	      blockScanner = new DataBlockScanner(this, (FSDataset)data, conf);
	    } else {
	      LOG.info("Periodic Block Verification is disabled because " +
	               reason + ".");
	    }

		this.connectToDnViaHostname = conf.getBoolean(
				DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
				DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
		LOG.debug("Connect to datanode via hostname is "
				+ connectToDnViaHostname);
	    
	    //create a servlet to serve full-file content
	    InetSocketAddress infoSocAddr = DataNode.getInfoAddr(conf);	    
	    String infoHost = infoSocAddr.getHostName();
	    int tmpInfoPort = infoSocAddr.getPort();
	    
//	    this.infoServer = new HttpServer("datanode", infoHost, tmpInfoPort,
//	        tmpInfoPort == 0, conf);
	    this.infoServer = (secureResources == null) 
	    	       ? new HttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0, 
	    	           conf, SecurityUtil.getAdminAcls(conf, DFSConfigKeys.DFS_ADMIN))
	    	       : new HttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0,
	    	           conf, SecurityUtil.getAdminAcls(conf, DFSConfigKeys.DFS_ADMIN),
	    	           secureResources.getListener());
	    if (conf.getBoolean("dfs.https.enable", false)) {
	      boolean needClientAuth = conf.getBoolean("dfs.https.need.client.auth", false);
	      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
	          "dfs.datanode.https.address", infoHost + ":" + 0));
	      Configuration sslConf = new Configuration(false);
	      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
	          "ssl-server.xml"));
	      this.infoServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
	    }
	    this.infoServer.addInternalServlet(null, "/streamFile/*", StreamFile.class);
	    this.infoServer.addInternalServlet(null, "/getFileChecksum/*",
	        FileChecksumServlets.GetServlet.class);
	    this.infoServer.setAttribute("datanode.blockScanner", blockScanner);
	    this.infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
	    this.infoServer.addServlet(null, "/blockScannerReport", 
	                               DataBlockScanner.Servlet.class);
	    this.infoServer.start();
	    // adjust info port
	    this.dnRegistration.setInfoPort(this.infoServer.getPort());
	    myMetrics = new DataNodeMetrics(conf, dnRegistration.getName());
  
	    // BlockTokenSecretManager is created here, but it shouldn't be
	    // used until it is initialized in register().
	    this.blockTokenSecretManager = new BlockTokenSecretManager(false,
	        0, 0);
	    
	    //init ipc server
	    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
	        conf.get("dfs.datanode.ipc.address"));
//	    ipcServer = RPC.getServer(this, ipcAddr.getHostName(), ipcAddr.getPort(), 
//	        conf.getInt("dfs.datanode.handler.count", 3), false, conf);	    
	    ipcServer = RPC.getServer(this, ipcAddr.getHostName(), ipcAddr.getPort(), 
	            conf.getInt("dfs.datanode.handler.count", 3), false, conf,
	            blockTokenSecretManager);
	    
	    // set service-level authorization security policy
		if (conf.getBoolean(
				CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
			PolicyProvider policyProvider = (PolicyProvider) (ReflectionUtils
					.newInstance(conf.getClass(
							PolicyProvider.POLICY_PROVIDER_CONFIG,
							HDFSPolicyProvider.class, PolicyProvider.class),
							conf));
			ipcServer.refreshServiceAcl(conf, policyProvider);
		}

	    dnRegistration.setIpcPort(ipcServer.getListenerAddress().getPort());
	    LOG.info("dnRegistration = " + dnRegistration); 
	    
	    pluginDispatcher = PluginDispatcher.createFromConfiguration(
	            conf, DFSConfigKeys.DFS_DATANODE_PLUGINS_KEY, DatanodePlugin.class);
	        pluginDispatcher.dispatchStart(this);
	    //end add guoleitao
  }
  
  
 // the following added by guoleitao  
  // connect to both name node if possible. 
  // If doWait is true, then return only when at least one handshake is
  // successful.
  //
  private synchronized NamespaceInfo handshake(boolean startup) throws IOException {
    NamespaceInfo nsInfo = null;
    boolean firstIsPrimary = false;
    do {
      if (startup) {
        InetSocketAddress addr = getNameNodeAddress(getConf());
        String addrStr = addr.getHostName() + ":" + addr.getPort();
        Stat stat = new Stat();
        try {
          String primaryAddress =
            zkClient.getPrimaryAvatarAddress(addrStr, stat, true);
          String firstNNAddress = nameAddr1.getHostName() + ":" +
            nameAddr1.getPort();
          firstIsPrimary = firstNNAddress.equalsIgnoreCase(primaryAddress);
        } catch (Exception ex) {
          LOG.error("Could not get the primary address from ZooKeeper", ex);
        }
      }
      try {
        if ((firstIsPrimary && startup) || !startup) {
          if (namenode1 == null) {
            namenode1 = (DatanodeProtocol) 
                             RPC.waitForProxy(DatanodeProtocol.class,
                               DatanodeProtocol.versionID,
                             nameAddr1, 
                             getConf());
            ((DatanodeProtocols)namenode).setDatanodeProtocol(namenode1, 0);
          }
          if (avatarnode1 == null) {
            avatarnode1 = (AvatarProtocol) 
                             RPC.waitForProxy(AvatarProtocol.class,
                               AvatarProtocol.versionID,
                             avatarAddr1, 
                             getConf());
          }
          if (startup) {
            nsInfo = handshake(namenode1, nameAddr1);
          }
        }
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + nameAddr1 + " not available yet, Zzzzz...");
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server timeout. " + nameAddr1);
      } catch (IOException ioe) {
        LOG.info("Problem connecting to server. " + nameAddr1, ioe);
      }
      try {
        if ((!firstIsPrimary && startup) || !startup) {
          if (namenode2 == null) {
            namenode2 = (DatanodeProtocol) 
                             RPC.waitForProxy(DatanodeProtocol.class,
                             DatanodeProtocol.versionID,
                             nameAddr2, 
                             getConf());
            ((DatanodeProtocols)namenode).setDatanodeProtocol(namenode2, 1);
          }
          if (avatarnode2 == null) {
            avatarnode2 = (AvatarProtocol) 
                             RPC.waitForProxy(AvatarProtocol.class,
                               AvatarProtocol.versionID,
                             avatarAddr2, 
                             getConf());
          }
          if (startup) {
            nsInfo = handshake(namenode2, nameAddr2);
          }
        }
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + nameAddr2 + " not available yet, Zzzzz...");
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server timeout. " + nameAddr2);
      } catch (IOException ioe) {
        LOG.info("Problem connecting to server. " + nameAddr2, ioe);
      }
    } while (startup && nsInfo == null && shouldRun);
    return nsInfo;
  }

  private NamespaceInfo handshake(DatanodeProtocol node,
                                  InetSocketAddress machine) throws IOException {
    NamespaceInfo nsInfo = new NamespaceInfo();
    while (shouldRun) {
      try {
        nsInfo = node.versionRequest();
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + machine);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
    // do not check version  
    // do not fail on incompatible build version
//    if( ! nsInfo.getBuildVersion().equals( Storage.getBuildVersion() )) {
//      errorMsg = "Incompatible build versions: namenode BV = " 
//        + nsInfo.getBuildVersion() + "; datanode BV = "
//        + Storage.getBuildVersion();
//      LOG.warn( errorMsg );
//    }
  
//    if (FSConstants.LAYOUT_VERSION != nsInfo.getLayoutVersion()) {
//      errorMsg = "Data-node and name-node layout versions must be the same."
//                  + "Expected: "+ FSConstants.LAYOUT_VERSION + 
//                  " actual "+ nsInfo.getLayoutVersion();
//      LOG.fatal(errorMsg);
//      try {
//        node.errorReport(dnRegistration,
//                         DatanodeProtocol.NOTIFY, errorMsg );
//      } catch( SocketTimeoutException e ) {  // namenode is busy        
//        LOG.info("Problem connecting to server: " + machine);
//      }
//      throw new IOException(errorMsg);
//    }
    if (!isPermittedVersion(nsInfo)) {
        String errorMsg = "Incompatible versions: namenode version " +
          nsInfo.getVersion() + " revision " + nsInfo.getRevision() +
          " datanode version " + VersionInfo.getVersion() + " revision " +
          VersionInfo.getRevision() + " and " +
          CommonConfigurationKeys.HADOOP_RELAXED_VERSION_CHECK_KEY +
          " is " + (relaxedVersionCheck ? "enabled" : "not enabled");
        LOG.fatal(errorMsg);
        notifyNamenode(DatanodeProtocol.NOTIFY, errorMsg);
        throw new IOException(errorMsg);
      }
      assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
        "Data-node and name-node layout versions must be the same."
        + "Expected: "+ FSConstants.LAYOUT_VERSION + " actual "+ nsInfo.getLayoutVersion();
    return nsInfo;
  }

  //end add guoleitao
  
//  @Override
//  protected void notifyNamenodeReceivedBlock(int namespaceId, Block block,
//      String delHint) throws IOException {
//    if (block == null) {
//      throw new IllegalArgumentException("Block is null");
//    }   
//    ((AvatarNamespaceManager)namespaceManager).notifyNamenodeReceivedBlock(
//        namespaceId, block, delHint);
//  }

//  @Override
//  protected void notifyNamenodeDeletedBlock(int namespaceId, Block block)
//      throws IOException {
//    if (block == null) {
//      throw new IllegalArgumentException("Block is null");
//    }   
//    ((AvatarNamespaceManager)namespaceManager).notifyNamenodeDeletedBlock(namespaceId, block); 
//  }

  /** TODO: will add more details to this later on
   * Manages OfferService objects for the data node namespaces.
   * Each namespace has two OfferServices, one for pirmary and one for standby.
   * Creation, removal, starting, stopping, shutdown on OfferService
   * objects must be done via APIs in this class.
   */
//  class AvatarNamespaceManager extends NamespaceManager {
//    private final Object refreshNamenodesLock = new Object();
//    AvatarNamespaceManager(
//        List<InetSocketAddress> nameAddrs0,
//        List<InetSocketAddress> nameAddrs1,
//        List<InetSocketAddress> avatarAddrs0,
//        List<InetSocketAddress> avatarAddrs1,
//        List<InetSocketAddress> defaultAddrs,
//        Collection<String> nameserviceIds) throws IOException {
//      Iterator<String> it = nameserviceIds.iterator();
//       for ( int i = 0; i<nameAddrs0.size(); i++) {
//         InetSocketAddress nameAddr0 = nameAddrs0.get(i);
//         String nameserviceId = it.hasNext()? it.next(): null;
//         nameNodeThreads.put(nameAddr0, 
//                             new ServicePair(nameAddr0, nameAddrs1.get(i),
//                                 avatarAddrs0.get(i), avatarAddrs1.get(i),
//                                 defaultAddrs.get(i), nameserviceId));
//       }
//      
//    }
//    
    /**
     * Notify both namenode(s) that we have received a block
     */
//    protected void notifyNamenodeReceivedBlock(int namespaceId, Block block,
//        String delHint) throws IOException {
//      NamespaceService servicePair = get(namespaceId);
//      if (servicePair == null) {
//        throw new IOException("Cannot locate OfferService thread for namespace="
//            + namespaceId);
//      }
//      servicePair.notifyNamenodeReceivedBlock(block, delHint);
//    }

    /**
     * Notify both namenode(s) that we have deleted a block
     */
//    protected void notifyNamenodeDeletedBlock(int namespaceId, Block block)
//    throws IOException {
//      NamespaceService servicePair = this.get(namespaceId);
//      if (servicePair == null) {
//        throw new IOException("Cannot locate OfferService thread for namespace="
//            + namespaceId);
//      }
//      servicePair.notifyNamenodeDeletedBlock(block);
//    }
//    
//    void refreshNamenodes(
//        List<InetSocketAddress> nameAddrs0,
//        List<InetSocketAddress> nameAddrs1,
//        List<InetSocketAddress> avatarAddrs0,
//        List<InetSocketAddress> avatarAddrs1,
//        List<InetSocketAddress> defaultAddrs,
//        Collection<String> nameserviceIds)
//        throws IOException, InterruptedException{
//      List<Integer> toStart = new ArrayList<Integer>();
//      List<String> toStartNameserviceIds = new ArrayList<String>();
//      List<NamespaceService> toStop = new ArrayList<NamespaceService>();
//      synchronized (refreshNamenodesLock) {
//        synchronized (this) {
//          for (InetSocketAddress nnAddr : nameNodeThreads.keySet()) {
//            if (!nameAddrs0.contains(nnAddr)){
//              toStop.add(nameNodeThreads.get(nnAddr));
//            }
//          }
//          Iterator<String> it = nameserviceIds.iterator();
//          for (int i = 0; i < nameAddrs0.size(); i++) {
//            String nameserviceId = it.hasNext()? it.next() : null;
//            if (!nameNodeThreads.containsKey(nameAddrs0.get(i))) {
//              toStart.add(i);
//              toStartNameserviceIds.add(nameserviceId);
//            }
//          }
//          it = toStartNameserviceIds.iterator();
//          for (Integer i : toStart) {
//            InetSocketAddress nameAddr0 = nameAddrs0.get(i);
//            nameNodeThreads.put(nameAddr0, 
//                new ServicePair(nameAddr0, nameAddrs1.get(i),
//                    avatarAddrs0.get(i), avatarAddrs1.get(i),
//                    defaultAddrs.get(i), it.next()));
//          }
//          for (NamespaceService nsos : toStop) {
//            remove(nsos);
//          }
//        }
//      }
//      for (NamespaceService nsos : toStop) {
//        nsos.stop();
//      }
//      startAll();
//    }
//  }

//  class ServicePair extends NamespaceService {
//    String defaultAddr;
//    InetSocketAddress nameAddr1;
//    InetSocketAddress nameAddr2;
//    DatanodeProtocol namenode1;
//    DatanodeProtocol namenode2;
//    AvatarProtocol avatarnode1;
//    AvatarProtocol avatarnode2;
//    InetSocketAddress avatarAddr1;
//    InetSocketAddress avatarAddr2;
//    boolean doneRegister1 = false;    // not yet registered with namenode1
//    boolean doneRegister2 = false;    // not yet registered with namenode2
//    OfferService offerService1;
//    OfferService offerService2;
//    volatile OfferService primaryOfferService = null;
//    Thread of1;
//    Thread of2;
//    int namespaceId;
//    String nameserviceId;
//    Thread spThread;
//    AvatarZooKeeperClient zkClient;
//    private NamespaceInfo nsInfo;
//    DatanodeRegistration nsRegistration;
//    private UpgradeManagerDatanode upgradeManager;
//    private volatile boolean initialized = false;
//    private volatile boolean shouldServiceRun = true;
//    volatile long lastBeingAlive = now();
//
//    private ServicePair(InetSocketAddress nameAddr1, InetSocketAddress nameAddr2,
//        InetSocketAddress avatarAddr1, InetSocketAddress avatarAddr2,
//        InetSocketAddress defaultAddr, String nameserviceId) {
//      this.nameAddr1 = nameAddr1;
//      this.nameAddr2 = nameAddr2;
//      this.avatarAddr1 = avatarAddr1;
//      this.avatarAddr2 = avatarAddr2;
//      this.defaultAddr = defaultAddr.getHostName() + ":" + defaultAddr.getPort();
//      this.nameserviceId = nameserviceId;
//      zkClient = new AvatarZooKeeperClient(getConf(), null);
//      this.nsRegistration = new DatanodeRegistration(getMachineName());
//    }
//    
//    private void setNamespaceInfo(NamespaceInfo nsinfo) {
//      this.nsInfo = nsinfo;
//      this.namespaceId = nsinfo.getNamespaceID();
//      namespaceManager.addNamespace(this);
//    }
//
//    private void setupNS() throws IOException {
//      // handshake with NN
//      NamespaceInfo nsInfo;
//      nsInfo = handshake(true);
//      setNamespaceInfo(nsInfo);
//      synchronized(AvatarDataNode.this){
//        setupNSStorage();
//      }
//      
//      nsRegistration.setIpcPort(ipcServer.getListenerAddress().getPort());
//      nsRegistration.setInfoPort(infoServer.getPort());
//    }
//    
//    private void setupNSStorage() throws IOException {
//      Configuration conf = getConf();
//      StartupOption startOpt = getStartupOption(conf);
//      assert startOpt != null : "Startup option must be set.";
//
//      boolean simulatedFSDataset = 
//        conf.getBoolean("dfs.datanode.simulateddatastorage", false);
//      
//      if (simulatedFSDataset) {
//        nsRegistration.setStorageID(storage.getStorageID()); //same as DN
//        nsRegistration.storageInfo.layoutVersion = FSConstants.LAYOUT_VERSION;
//        nsRegistration.storageInfo.namespaceID = nsInfo.namespaceID;
//      } else {
//        // read storage info, lock data dirs and transition fs state if necessary      
//        // first do it at the top level dataDirs
//        // This is done only once when among all namespaces
//        storage.recoverTransitionRead(DataNode.getDataNode(), nsInfo, dataDirs, startOpt);
//        // Then do it for this namespace's directory
//        storage.recoverTransitionRead(DataNode.getDataNode(), nsInfo.namespaceID, nsInfo, dataDirs, 
//            startOpt, nameserviceId);
//        
//        LOG.info("setting up storage: namespaceId="
//            + namespaceId + ";lv=" + storage.layoutVersion + ";nsInfo="
//            + nsInfo);
//
//        nsRegistration.setStorageInfo(
//            storage.getNStorage(nsInfo.namespaceID), storage.getStorageID());
//        data.initialize(storage);
//        
//      }
//      data.addNamespace(namespaceId, storage.getNameSpaceDataDir(namespaceId), conf);
//      if (blockScanner != null) {
//        blockScanner.start();
//        blockScanner.addNamespace(namespaceId);
//      }
//    }
//    
//    @Override
//    public UpgradeManagerDatanode getUpgradeManager() {
//      synchronized (AvatarDataNode.this) {
//      if(upgradeManager == null)
//        upgradeManager = 
//          new UpgradeManagerDatanode(DataNode.getDataNode(), namespaceId);
//      }
//      return upgradeManager;
//    }
//    
//    public void processUpgradeCommand(UpgradeCommand comm)
//    throws IOException {
//      assert upgradeManager != null : "DataNode.upgradeManager is null.";
//      upgradeManager.processUpgradeCommand(comm);
//    }
//
//    /**
//     * Start distributed upgrade if it should be initiated by the data-node.
//     */
//    private void startDistributedUpgradeIfNeeded() throws IOException {
//      UpgradeManagerDatanode um = getUpgradeManager();
//
//      if(!um.getUpgradeState())
//        return;
//      um.setUpgradeState(false, um.getUpgradeVersion());
//      um.startUpgrade();
//      return;
//    }
//
//    public void start() {
//      if ((spThread != null) && (spThread.isAlive())) {
//        //Thread is started already
//        return;
//      }
//      spThread = new Thread(this, dnThreadName + " for namespace " + namespaceId);
//      spThread.setDaemon(true);
//      spThread.start();
//
//    }
//    
//    public void stop() {
//      stopServices();
//      if (spThread != null) {
//        spThread.interrupt();
//      }
//    }
//    
//    /** stop two offer services */
//    private void stopServices() {
//      this.shouldServiceRun = false;
//      LOG.info("stop services " + this.nameserviceId);
//      if (offerService1 != null) {
//        offerService1.stop();
//      }
//      if (of1 != null) {
//        of1.interrupt();
//      }
//      if (offerService2 != null) {
//        offerService2.stop();
//      }
//      if (of2 != null) {
//        of2.interrupt();
//      }
//      if (zkClient != null) {
//        try {
//          zkClient.shutdown();
//        } catch (InterruptedException ie) {
//          LOG.warn("Zk shutdown is interrupted: ", ie);
//        }
//      }
//    }
//    
//    public void join() {
//      joinServices();
//      if (spThread != null) {
//        try {
//          spThread.join();
//        } catch (InterruptedException ie) {
//        }
//        spThread = null;
//      }
//    }
//    
//    /** Join two offer services */
//    private void joinServices() {
//      if (of1 != null) {
//        try {
//          of1.join();
//        } catch (InterruptedException ie) {
//        }
//      }
//      if (of2 != null) {
//        try {
//          of2.join();
//        } catch (InterruptedException ie) {
//        }
//      }
//    }
//    
//    public void cleanUp() {
//      if(upgradeManager != null)
//        upgradeManager.shutdownUpgrade();
//      
//      namespaceManager.remove(this);
//      shouldServiceRun = false;
//      try {
//        RPC.stopProxy(namenode1);
//      } catch (Exception e){
//        LOG.warn("Exception stop the namenode RPC threads", e);
//      }
//      try {
//        RPC.stopProxy(namenode2);
//      } catch (Exception e){
//        LOG.warn("Exception stop the namenode RPC threads", e);
//      }
//      if (blockScanner != null) {
//        blockScanner.removeNamespace(this.getNamespaceId());
//      }
//      if (data != null) { 
//        data.removeNamespace(this.getNamespaceId());
//      }
//      if (storage != null) {
//        storage.removeNamespaceStorage(this.getNamespaceId());
//      }
//    }
//    
//    public void shutdown() {
//      stop();
//      join();
//    }
//
//    
//  // connect to both name node if possible. 
//  // If doWait is true, then return only when at least one handshake is
//  // successful.
//  //
//  private NamespaceInfo handshake(boolean startup) throws IOException {
//    NamespaceInfo nsInfo = null;
//    boolean firstIsPrimary = false;
//    do {
//      if (startup) {
//        // The startup option is used when the datanode is first created
//        // We only need to connect to the primary at this point and as soon
//        // as possible. So figure out who the primary is from the ZK
//        Stat stat = new Stat();
//        try {
//          String primaryAddress =
//            zkClient.getPrimaryAvatarAddress(defaultAddr, stat, true);
//          String firstNNAddress = nameAddr1.getHostName() + ":" +
//            nameAddr1.getPort();
//          firstIsPrimary = firstNNAddress.equalsIgnoreCase(primaryAddress);
//        } catch (Exception ex) {
//          LOG.error("Could not get the primary address from ZooKeeper", ex);
//        }
//      }
//      try {
//        if ((firstIsPrimary && startup) || !startup) {
//          // only try to connect to the first NN if it is not the
//          // startup connection or if it is primary on startup
//          // This way if it is standby we are not wasting datanode startup time
//          if (namenode1 == null) {
//            namenode1 = (DatanodeProtocol) 
//                             RPC.getProxy(DatanodeProtocol.class,
//                               DatanodeProtocol.versionID,
//                             nameAddr1, 
//                             getConf());
//          }
//          if (avatarnode1 == null) {
//            avatarnode1 = (AvatarProtocol) 
//                             RPC.getProxy(AvatarProtocol.class,
//                               AvatarProtocol.versionID,
//                             avatarAddr1, 
//                             getConf());
//          }
//          if (startup) {
//            nsInfo = handshake(namenode1, nameAddr1);
//          }
//        }
//      } catch(ConnectException se) {  // namenode has not been started
//        LOG.info("Server at " + nameAddr1 + " not available yet, Zzzzz...");
//      } catch(SocketTimeoutException te) {  // namenode is busy
//        LOG.info("Problem connecting to server timeout. " + nameAddr1);
//      } catch (IOException ioe) {
//        LOG.info("Problem connecting to server. " + nameAddr1, ioe);
//      }
//      try {
//        if ((!firstIsPrimary && startup) || !startup) {
//          if (namenode2 == null) {
//            namenode2 = (DatanodeProtocol) 
//                             RPC.getProxy(DatanodeProtocol.class,
//                             DatanodeProtocol.versionID,
//                             nameAddr2, 
//                             getConf());
//          }
//          if (avatarnode2 == null) {
//            avatarnode2 = (AvatarProtocol) 
//                             RPC.getProxy(AvatarProtocol.class,
//                               AvatarProtocol.versionID,
//                             avatarAddr2, 
//                             getConf());
//          }
//          if (startup) {
//            nsInfo = handshake(namenode2, nameAddr2);
//          }
//        }
//      } catch(ConnectException se) {  // namenode has not been started
//        LOG.info("Server at " + nameAddr2 + " not available yet, Zzzzz...");
//      } catch(SocketTimeoutException te) {  // namenode is busy
//        LOG.info("Problem connecting to server timeout. " + nameAddr2);
//      } catch (RemoteException re) {
//        handleRegistrationError(re);
//      } catch (IOException ioe) {
//        LOG.info("Problem connecting to server. " + nameAddr2, ioe);
//      }
//    } while (startup && nsInfo == null && shouldServiceRun);
//    return nsInfo;
//  }
//
//  private NamespaceInfo handshake(DatanodeProtocol node,
//                                  InetSocketAddress machine) throws IOException {
//    NamespaceInfo nsInfo = new NamespaceInfo();
//    while (shouldServiceRun) {
//      try {
//        nsInfo = node.versionRequest();
//        break;
//      } catch(SocketTimeoutException e) {  // namenode is busy
//        LOG.info("Problem connecting to server: " + machine);
//        try {
//          Thread.sleep(1000);
//        } catch (InterruptedException ie) {}
//      }
//    }
//    String errorMsg = null;
//    // do not fail on incompatible build version
//    if( ! nsInfo.getBuildVersion().equals( Storage.getBuildVersion() )) {
//      errorMsg = "Incompatible build versions: namenode BV = " 
//        + nsInfo.getBuildVersion() + "; datanode BV = "
//        + Storage.getBuildVersion();
//      LOG.warn( errorMsg );
//    }
//    if (FSConstants.LAYOUT_VERSION != nsInfo.getLayoutVersion()) {
//      errorMsg = "Data-node and name-node layout versions must be the same."
//                  + "Expected: "+ FSConstants.LAYOUT_VERSION + 
//                  " actual "+ nsInfo.getLayoutVersion();
//      LOG.fatal(errorMsg);
//      try {
//        node.errorReport(nsRegistration,
//                         DatanodeProtocol.NOTIFY, errorMsg );
//      } catch( SocketTimeoutException e ) {  // namenode is busy        
//        LOG.info("Problem connecting to server: " + machine);
//      }
//      shutdownDN();
//      throw new IOException(errorMsg);
//    }
//    return nsInfo;
//  }
//
//  /**
//   * Returns true if we are able to successfully register with namenode
//   */
//  boolean register(DatanodeProtocol node, InetSocketAddress machine) 
//    throws IOException {
//    if (nsRegistration.getStorageID().equals("")) {
//      setNewStorageID(nsRegistration);
//    }
//
//    DatanodeRegistration tmp = new DatanodeRegistration(nsRegistration.getName());
//    tmp.setInfoPort(nsRegistration.getInfoPort());
//    tmp.setIpcPort(nsRegistration.getIpcPort());
//    boolean simulatedFSDataset = 
//        conf.getBoolean("dfs.datanode.simulateddatastorage", false);
//    if (simulatedFSDataset) {
//      tmp.setStorageID(storage.getStorageID()); //same as DN
//      tmp.storageInfo.layoutVersion = FSConstants.LAYOUT_VERSION;
//      tmp.storageInfo.namespaceID = nsInfo.namespaceID;
//    } else {
//      tmp.setStorageInfo(storage.getNStorage(namespaceId), storage.getStorageID());
//    }
//
//    // reset name to machineName. Mainly for web interface.
//    tmp.name = machineName + ":" + nsRegistration.getPort();
//    try {
//      tmp = node.register(tmp);
//      // if we successded registering for the first time, then we update
//      // the global registration objct
//      if (!doneRegister1 && !doneRegister2) {
//        nsRegistration = tmp;
//      }
//    } catch(SocketTimeoutException e) {  // namenode is busy
//      LOG.info("Problem connecting to server: " + machine);
//      return false;
//    }
//
//    assert ("".equals(storage.getStorageID()) 
//            && !"".equals(nsRegistration.getStorageID()))
//            || storage.getStorageID().equals(nsRegistration.getStorageID()) :
//            "New storageID can be assigned only if data-node is not formatted";
//    if (storage.getStorageID().equals("")) {
//      storage.setStorageID(nsRegistration.getStorageID());
//      storage.writeAll();
//      LOG.info("New storage id " + nsRegistration.getStorageID()
//          + " is assigned to data-node " + nsRegistration.getName());
//    }
//    if(! storage.getStorageID().equals(nsRegistration.getStorageID())) {
//      throw new IOException("Inconsistent storage IDs. Name-node returned "
//          + nsRegistration.getStorageID() 
//          + ". Expecting " + storage.getStorageID());
//    }
//
//    if (supportAppends) {
//      Block[] blocks = data.getBlocksBeingWrittenReport(namespaceId);
//      if (blocks != null && blocks.length != 0) {
//        long[] blocksAsLong =
//          BlockListAsLongs.convertToArrayLongs(blocks);
//        BlockReport bbwReport = new BlockReport(blocksAsLong);
//        node.blocksBeingWrittenReport(nsRegistration, bbwReport);
//      }
//    }
//    return true;
//  }
//  
//  boolean isPrimaryOfferService(OfferService service) {
//    return primaryOfferService == service;
//  }
//  
//  void setPrimaryOfferService(OfferService service) {
//    this.primaryOfferService = service;
//    if (service != null)
//      LOG.info("Primary namenode is set to be " + service.avatarnodeAddress);
//    else {
//      LOG.info("Failover has happened. Stop accessing commands from " +
//      		"either namenode until the new primary is completely in" +
//      		"sync with all the datanodes");
//    }
//  }
//
//  @Override
//  public void run() {
//    LOG.info(nsRegistration + "In AvatarDataNode.run, data = " + data);
//
//    try {
//    // set up namespace
//    try {
//      setupNS();
//    } catch (IOException ioe) {
//      // Initial handshake, storage recovery or registration failed
//      LOG.fatal(nsRegistration + " initialization failed for namespaceId "
//          + namespaceId, ioe);
//      return;
//    }
//    
//    while (shouldServiceRun) {
//      try {
//        // try handshaking with any namenode that we have not yet tried
//        handshake(false);
//
//        if (avatarnode1 != null && namenode1 != null && !doneRegister1 &&
//            register(namenode1, nameAddr1)) {
//          doneRegister1 = true;
//          offerService1 = new OfferService(AvatarDataNode.this, this,
//                                           namenode1, nameAddr1, 
//                                           avatarnode1, avatarAddr1);
//          of1 = new Thread(offerService1, "OfferService1 " + nameAddr1);
//          of1.start();
//        }
//        if (avatarnode2 != null && namenode2 != null && !doneRegister2 &&
//            register(namenode2, nameAddr2)) {
//          doneRegister2 = true;
//          offerService2 = new OfferService(AvatarDataNode.this, this,
//                                           namenode2, nameAddr2,
//                                           avatarnode2, avatarAddr2);
//          of2 = new Thread(offerService2, "OfferService2 " + nameAddr2);
//          of2.start();
//        }
//
//        this.initialized = true;
//        startDistributedUpgradeIfNeeded();
//      } catch (RemoteException re) {
//        handleRegistrationError(re);
//      } catch (Exception ex) {
//        LOG.error("Exception: ", ex);
//      }
//      if (shouldServiceRun && !shutdown) {
//        try {
//          Thread.sleep(5000);
//        } catch (InterruptedException ie) {
//        }
//      }
//    }
//    } finally {
//
//    LOG.info(nsRegistration + ":Finishing AvatarDataNode in: "+data);
//    stopServices();
//    joinServices();
//    cleanUp();
//    }
//  }
//
//  /**
//   * Notify both namenode(s) that we have received a block
//   */
//  @Override
//  public void notifyNamenodeReceivedBlock(Block block, String delHint) {
//    if (offerService1 != null) {
//      offerService1.notifyNamenodeReceivedBlock(block, delHint);
//    }
//    if (offerService2 != null) {
//      offerService2.notifyNamenodeReceivedBlock(block, delHint);
//    }
//  }
//
//  /**
//   * Notify both namenode(s) that we have deleted a block
//   */
//  @Override
//  public void notifyNamenodeDeletedBlock(Block block) {
//    if (offerService1 != null) {
//      offerService1.notifyNamenodeDeletedBlock(block);
//    }
//    if (offerService2 != null) {
//      offerService2.notifyNamenodeDeletedBlock(block);
//    }
//  }
//
//  /**
//   * Update received and retry list, when blocks are deleted
//   */
//  void removeReceivedBlocks(Block[] list) {
//    if (offerService1 != null) {
//      offerService1.removeReceivedBlocks(list);
//    }
//    if (offerService2 != null) {
//      offerService2.removeReceivedBlocks(list);
//    }
//  }
//
//  @Override
//  public DatanodeRegistration getNsRegistration() {
//    return nsRegistration;
//  }
//
//  @Override
//  public DatanodeProtocol getDatanodeProtocol() {
//    return this.primaryOfferService.namenode;
//  }
//
//  @Override
//  public InetSocketAddress getNNSocketAddress() {
//    return this.nameAddr1;
//  }
//
//  @Override
//  public int getNamespaceId() {
//    return this.namespaceId;
//  }
//  
//  @Override
//  public String getNameserviceId() {
//    return this.nameserviceId;
//  }
//
//  @Override
//  public boolean initialized() {
//    return initialized;
//  }
//
//  @Override
//  public boolean isAlive() {
//    return shouldServiceRun && spThread.isAlive();
//  }
//
//  @Override
//  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
//    if (this.offerService1 != null)
//      this.offerService1.reportBadBlocks(blocks);
//    if (this.offerService2 != null)
//      this.offerService2.reportBadBlocks(blocks);
//  }
//
  @Override
//  public LocatedBlock syncBlock(Block block, List<BlockRecord> syncList,
//      boolean closeFile, List<InterDatanodeProtocol> datanodeProxies)
//      throws IOException {//del by guoleitao
  public LocatedBlock syncBlock(Block block, List<BlockRecord> syncList,
      DatanodeInfo[] targets,  boolean closeFile)
      throws IOException {//add by guoleitao
    if (offerService1 != null && isPrimaryOfferService(offerService1))
//      return offerService1.syncBlock(block, syncList, closeFile, datanodeProxies);//del by guoleitao
    return offerService1.syncBlock(block, syncList, targets, closeFile);
    if (offerService2 != null && isPrimaryOfferService(offerService2))
//      return offerService2.syncBlock(block, syncList, closeFile, datanodeProxies);//del by guoleitao
    return offerService2.syncBlock(block, syncList, targets, closeFile);
    return null;
  }
  
  @Override
  public void scheduleBlockReport(long delay) {
    if (this.offerService1 != null)
      this.offerService1.scheduleBlockReport(delay);
    if (this.offerService2 != null)
      this.offerService2.scheduleBlockReport(delay);
  }
//  
//  // Only use for testing
//  public void scheduleBlockReceivedAndDeleted(long delay) {
//    if (this.offerService1 != null)
//      this.offerService1.scheduleBlockReceivedAndDeleted(delay);
//    if (this.offerService2 != null)
//      this.offerService2.scheduleBlockReceivedAndDeleted(delay);
//  }
//
//  }
 /**
  * Tells the datanode to start the shutdown process.
  */
//  public synchronized void shutdownDN() {
//    shutdown = true;
//    if (namespaceManager != null) {
//      namespaceManager.stopAll();
//    }
//  }
  
  
  // the following add by guoleitao
  /**
   * Returns true if we are able to successfully register with namenode
   */
  boolean register(DatanodeProtocol node, InetSocketAddress machine) 
    throws IOException {
    if (dnRegistration.getStorageID().equals("")) {
      setNewStorageID(dnRegistration);
    }

    DatanodeRegistration tmp = new DatanodeRegistration(dnRegistration.getName());
    tmp.setStorageInfo(storage);
    tmp.setInfoPort(dnRegistration.getInfoPort());
    tmp.setIpcPort(dnRegistration.getIpcPort());

    // reset name to machineName. Mainly for web interface.
    tmp.name = machineName + ":" + dnRegistration.getPort();
    try {
      tmp = node.register(tmp);
      // if we successded registering for the first time, then we update
      // the global registration objct
      if (!doneRegister1 && !doneRegister2) {
        dnRegistration = tmp;
      }
    } catch(SocketTimeoutException e) {  // namenode is busy
      LOG.info("Problem connecting to server: " + machine);
      return false;
    }

    assert ("".equals(storage.getStorageID()) 
            && !"".equals(dnRegistration.getStorageID()))
            || storage.getStorageID().equals(dnRegistration.getStorageID()) :
            "New storageID can be assigned only if data-node is not formatted";
    if (storage.getStorageID().equals("")) {
      storage.setStorageID(dnRegistration.getStorageID());
      storage.writeAll();
      LOG.info("New storage id " + dnRegistration.getStorageID()
          + " is assigned to data-node " + dnRegistration.getName());
    }
    if(! storage.getStorageID().equals(dnRegistration.getStorageID())) {
      throw new IOException("Inconsistent storage IDs. Name-node returned "
          + dnRegistration.getStorageID() 
          + ". Expecting " + storage.getStorageID());
    }
    
    if (!isBlockTokenInitialized) {
        /* first time registering with NN */
        ExportedBlockKeys keys = dnRegistration.exportedKeys;
        this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
        if (isBlockTokenEnabled) {
          long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
          long blockTokenLifetime = keys.getTokenLifetime();
          LOG.info("Block token params received from NN: keyUpdateInterval="
              + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
              + blockTokenLifetime / (60 * 1000) + " min(s)");
          blockTokenSecretManager.setTokenLifetime(blockTokenLifetime);
        }
        isBlockTokenInitialized = true;
      }

      if (isBlockTokenEnabled) {
        blockTokenSecretManager.setKeys(dnRegistration.exportedKeys);
        dnRegistration.exportedKeys = ExportedBlockKeys.DUMMY_KEYS;
      }
    
    //add by guoleitao
//  if (supportAppends) {
  Block[] blocks = data.getBlocksBeingWrittenReport();
  if (blocks != null && blocks.length != 0) {
    long[] blocksAsLong =
      BlockListAsLongs.convertToArrayLongs(blocks);
//    BlockReport bbwReport = new BlockReport(blocksAsLong);
    node.blocksBeingWrittenReport(dnRegistration, blocksAsLong);  
}

//  // random short delay - helps scatter the BR from all DNs
//  // - but we can start generating the block report immediately
//  data.requestAsyncBlockReport();
//  scheduleBlockReport(initialBlockReportDelay);

    //end add guoleitao
    return  true;
  }
  
  boolean isPrimaryOfferService(OfferService service) {
    return primaryOfferService == service;
  }
  
  synchronized void setPrimaryOfferService(OfferService service) {
    this.primaryOfferService = service;
    if (service != null)
      LOG.info("Primary namenode is set to be " + service.avatarnodeAddress);
    else {
      LOG.warn("Failover has happened. Stop accessing commands from " +
      		"either namenode until the new primary is completely in" +
      		"sync with all the datanodes");
    }
  }

  @Override
  public void run() {
    LOG.info(dnRegistration + "In AvatarDataNode.run, data = " + data);

    // start dataXceiveServer
    dataXceiverServer.start();
    ipcServer.start();

    while (shouldRun && !shutdown) {
      try {

        // try handshaking with any namenode that we have not yet tried
        handshake(false);

        if (avatarnode1 != null && namenode1 != null && !doneRegister1 &&
            register(namenode1, nameAddr1)) {
          LOG.info("start OfferService to namenode: "+nameAddr1);
          doneRegister1 = true;
          offerService1 = new OfferService(this, namenode1, nameAddr1, 
                                           avatarnode1, avatarAddr1);
          of1 = new Thread(offerService1, "OfferService1 " + nameAddr1);
          of1.start();
        }
        if (avatarnode2 != null && namenode2 != null && !doneRegister2 &&
            register(namenode2, nameAddr2)) {
          LOG.info("start OfferService to namenode: "+nameAddr2);
          doneRegister2 = true;
          offerService2 = new OfferService(this, namenode2, nameAddr2,
                                           avatarnode2, avatarAddr2);
          of2 = new Thread(offerService2, "OfferService2 " + nameAddr2);
          of2.start();
        }

        startDistributedUpgradeIfNeeded();

        // start block scanner
        if (blockScanner != null && blockScannerThread == null &&
            upgradeManager.isUpgradeCompleted()) {
          LOG.info("Starting Periodic block scanner.");
          blockScannerThread = new Daemon(blockScanner);
          blockScannerThread.start();
        }
        //add by guoleitao to monitor failover
        if (nnm == null && nnmThread == null) {
          LOG.info("Starting namenode failover monitoring thread NNMonitor.");
          nnm = new NNMonitor(this, this.getConf());
          nnmThread = new Daemon(nnm);
          nnmThread.start();
        }
        //end add guoleitao
      } catch (Exception ex) {
        LOG.error("Exception: " + StringUtils.stringifyException(ex));
      }
      if (shouldRun && !shutdown) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
        }
      }
    }

    LOG.info(dnRegistration + ":Finishing AvatarDataNode in: "+data);
    shutdown();
  }

  /**
   * Notify both namenode(s) that we have received a block
   */
  @Override
  protected void notifyNamenodeReceivedBlock(Block block, String delHint) {
    if (offerService1 != null) {
      offerService1.notifyNamenodeReceivedBlock(block, delHint);
    }
    if (offerService2 != null) {
      offerService2.notifyNamenodeReceivedBlock(block, delHint);
    }
  }
  
  //this fun can not be invoked by datanode, there is not this fun in cdh3u4, guoleitao
  public void notifyNamenodeDeletedBlock(Block block) {
    if (offerService1 != null) {
      offerService1.notifyNamenodeDeletedBlock(block);
    }
    if (offerService2 != null) {
      offerService2.notifyNamenodeDeletedBlock(block);
    }
  }


  /**
   * Notify both namenode(s) that we have deleted a block
   */
  void removeReceivedBlocks(Block[] list) {
    if (offerService1 != null) {
      offerService1.removeReceivedBlocks(list);
    }
    if (offerService2 != null) {
      offerService2.removeReceivedBlocks(list);
    }
  }

  /**
   * Start distributed upgrade if it should be initiated by the data-node.
   */
  private void startDistributedUpgradeIfNeeded() throws IOException {
    UpgradeManagerDatanode um = DataNode.getDataNode().upgradeManager;
    assert um != null : "DataNode.upgradeManager is null.";
    if(!um.getUpgradeState())
      return;
    um.setUpgradeState(false, um.getUpgradeVersion());
    um.startUpgrade();
    return;
  }

  void transferBlocks(Block blocks[], DatanodeInfo xferTargets[][]) {
    for (int i = 0; i < blocks.length; i++) {
      try {
        transferBlockMethod.invoke(this, blocks[i], xferTargets[i]);
      } catch (java.lang.IllegalAccessException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
      } catch (java.lang.reflect.InvocationTargetException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
      }
    }
  }

 /**
  * Tells the datanode to start the shutdown process.
  */
  public synchronized void shutdownDN() {
    shutdown = true;
    if (dataNodeThread != null) {
      dataNodeThread.interrupt();
    }
  }
 /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   * This can be called from one of the Offer services thread
   * or from the DataNode main thread.
   */
  @Override
  public synchronized void shutdown() {
   LOG.info("stop avatar datanode service on "+this.getHostName());
    // add by guoleitao
    LOG.info("Stop the NNMonitor thread.");
    if (nnm != null) {
      nnm.stop();
      nnmThread.interrupt();
      try {
        nnmThread.join();
      } catch (InterruptedException iex) {
      }
    }
    // end add guoleitao
   if (of1 != null) {
      offerService1.stop();
      of1.interrupt();
      try {
        of1.join();
      } catch (InterruptedException ie) {
      }
    }
    if (of2 != null) {
      offerService2.stop();
      of2.interrupt();
      try {
        of2.join();
      } catch (InterruptedException ie) {
      }
    }
    if(zkClient !=null){
    	try {
			zkClient.shutdown();
		} catch (InterruptedException e) {
			LOG.warn("ZK shutdown is interrupted: ", e);
		}
   }
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down DataNode", e);
      }
    }
    ((DatanodeProtocols)this.namenode).shutdown();
    this.namenode = null;
    super.shutdown();
    if (storage != null) {
      try {
        this.storage.unlockAll();
      } catch (IOException ie) {
      }
    }
    if (dataNodeThread != null &&
        Thread.currentThread() != dataNodeThread) {
      dataNodeThread.interrupt();
      try {
        dataNodeThread.join();
      } catch (InterruptedException ie) {
      }
    }
  }

  public static void runDatanodeDaemon(AvatarDataNode dn) throws IOException {
    if (dn != null) {
    	  dn.pluginDispatcher.dispatchCall(
    		        new SingleArgumentRunnable<DatanodePlugin>() {
    		          public void run(DatanodePlugin p) { p.initialRegistrationComplete(); }
    		        });
      dn.dataNodeThread = new Thread(dn, dn.dnThreadName);
      dn.dataNodeThread.setDaemon(true); // needed for JUnit testing
      dn.dataNodeThread.start();
    }
  } //del by guoleitao, cdh3u4 has this func

  void join() {
    if (dataNodeThread != null) {
      try {
        dataNodeThread.join();
      } catch (InterruptedException e) {}
    }
  }
  //end add  
  
  DataStorage getStorage() {
    return storage;
  }

  private static void printUsage() {
    System.err.println("Usage: java DataNode");
    System.err.println("           [-rollback]");
  }

  /**
   * Parse and verify command line arguments and set configuration parameters.
   *
   * @return false if passed argements are incorrect
   */
  private static boolean parseArguments(String args[],
                                        Configuration conf) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
        LOG.error("-r, --rack arguments are not supported anymore. RackID " +
            "resolution is handled by the NameNode.");
        System.exit(-1);
      } else if ("-rollback".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if ("-regular".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else
        return false;
    }
    setStartupOption(conf, startOpt);
    return true;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.datanode.startup", opt.toString());
  }

  /**
   * Returns the IP address of the namenode
   */
//  static InetSocketAddress getNameNodeAddress(Configuration conf,
//                                                      String cname, String rpcKey, String cname2) {
//    String fs = conf.get(cname);
//    String fs1 = conf.get(rpcKey);
//    String fs2 = conf.get(cname2);
//    Configuration newconf = new Configuration(conf);
//    newconf.set("fs.default.name", fs);
//    if (fs1 != null) {
//      newconf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, fs1);
//    }
//    if (fs2 != null) {
//      newconf.set("dfs.namenode.dn-address", fs2);
//    }
//    return DataNode.getNameNodeAddress(newconf);
//  }

  /** add by guoleitao
   * Returns the IP address of the namenode
   */
  static InetSocketAddress getNameNodeAddress(Configuration conf,
                                                      String cname, String cname2) {
    String fs = conf.get(cname);
    String fs2 = conf.get(cname2);
    Configuration newconf = new Configuration(conf);
    newconf.set("fs.default.name", fs);
    if (fs2 != null) {
      newconf.set("dfs.namenode.dn-address", fs2);
    }
//    return DataNode.getNameNodeAddress(newconf);
    return getNameNodeAddress(newconf);//add by guoleitao
  }
  
  /**
   * This method returns the address namenode uses to communicate with
   * datanodes. If this address is not configured the default NameNode
   * address is used, as it is running only one RPC server.
   * If it is running multiple servers this address cannot be used by clients!!
   * @param conf
   * @return
   */
  public static InetSocketAddress getNameNodeAddress(Configuration conf) {
    InetSocketAddress addr = null;
    addr = NameNode.getDNProtocolAddress(conf);
    if (addr != null) {
      return addr;
    }
    return NameNode.getAddress(conf);
  }
  
  @Override
  public InetSocketAddress getNameNodeAddr() {
    return NameNode.getAddress(getConf());
  }

  /**
   * Returns the IP:port address of the avatar node
   */
  private static InetSocketAddress getAvatarNodeAddress(Configuration conf,
                                                        String cname) {
    String fs = conf.get(cname);
    Configuration newconf = new Configuration(conf);
    newconf.set("fs.default.name", fs);
    return AvatarNode.getAddress(newconf);
  }

  /**
   * Returns the IP:port address of the avatar node
   */
//  private static List<InetSocketAddress> getAvatarNodeAddresses(String suffix,
//      Configuration conf, Collection<String> serviceIds) throws IOException{
//    List<InetSocketAddress> namenodeAddresses = getRPCAddresses(suffix,
//        conf, serviceIds, FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);
//    List<InetSocketAddress> avatarnodeAddresses = 
//      new ArrayList<InetSocketAddress>(namenodeAddresses.size());
//    for (InetSocketAddress namenodeAddress : namenodeAddresses) {
//      avatarnodeAddresses.add(
//          new InetSocketAddress(namenodeAddress.getHostName(),conf.getInt(
//              "dfs.avatarnode.port", namenodeAddress.getPort() + 1)));
//    }
//    return avatarnodeAddresses;
//  }

  public static AvatarDataNode makeInstance(String[] dataDirs, Configuration conf)
    throws IOException {
    ArrayList<File> dirs = new ArrayList<File>();
    for (int i = 0; i < dataDirs.length; i++) {
      File data = new File(dataDirs[i]);
      try {
        DiskChecker.checkDir(data);
        dirs.add(data);
      } catch(DiskErrorException e) {
        LOG.warn("Invalid directory in dfs.data.dir: " + e.getMessage());
      }
    }
    if (dirs.size() > 0) {
      String dnThreadName = "AvatarDataNode: [" +
        StringUtils.arrayToString(dataDirs) + "]";
      return new AvatarDataNode(conf, dirs, dnThreadName);
    }
    LOG.error("All directories in dfs.data.dir are invalid.");
    return null;
  }

  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon(DataNode)} subsequently. 
   */
  public static AvatarDataNode instantiateDataNode(String args[],
                                      Configuration conf) throws IOException {
    if (conf == null)
      conf = new Configuration();
    if (!parseArguments(args, conf)) {
      printUsage();
      return null;
    }
    if (conf.get("dfs.network.script") != null) {
      LOG.error("This configuration for rack identification is not supported" +
          " anymore. RackID resolution is handled by the NameNode.");
      System.exit(-1);
    }
    String[] dataDirs = conf.getStrings("dfs.data.dir");
    return makeInstance(dataDirs, conf);
  }

  public static AvatarDataNode createDataNode(String args[],
                                 Configuration conf) throws IOException {
    AvatarDataNode dn = instantiateDataNode(args, conf);
//    dn.runDatanodeDaemon(); //del by guoleitao
    runDatanodeDaemon(dn); //add by guoleitao
    return dn;
  }
  
//  @Override
//  public void refreshNamenodes(Configuration conf) throws IOException {
//    LOG.info("refresh namenodes");
//    try {
//      Collection<String> serviceIds = DFSUtil.getNameServiceIds(conf);
//      List<InetSocketAddress> defaultNameAddrs = 
//          AvatarDataNode.getDatanodeProtocolAddresses(conf, serviceIds);
//      List<InetSocketAddress> nameAddrs0 = 
//          AvatarDataNode.getRPCAddresses("0", conf, serviceIds,
//              NameNode.DATANODE_PROTOCOL_ADDRESS,
//              FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);
//      List<InetSocketAddress> nameAddrs1 =
//          AvatarDataNode.getRPCAddresses("1", conf, serviceIds,
//              NameNode.DATANODE_PROTOCOL_ADDRESS, 
//              FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY);
//      List<InetSocketAddress> avatarAddrs0 =
//          AvatarDataNode.getAvatarNodeAddresses("0", conf, serviceIds);
//      List<InetSocketAddress> avatarAddrs1 =
//          AvatarDataNode.getAvatarNodeAddresses("1", conf, serviceIds);
//      ((AvatarNamespaceManager)namespaceManager).refreshNamenodes(
//          nameAddrs0, nameAddrs1,
//          avatarAddrs0, avatarAddrs1, 
//          defaultNameAddrs, serviceIds);
//    } catch (InterruptedException e) {
//      throw new IOException(e.getCause());
//    }
//  }

  void handleRegistrationError(RemoteException re) {
    // If either the primary or standby NN throws these exceptions, this
    // datanode will exit. I think this is the right behaviour because
    // the excludes list on both namenode better be the same.
    String reClass = re.getClassName(); 
    if (UnregisteredDatanodeException.class.getName().equals(reClass) ||
        DisallowedDatanodeException.class.getName().equals(reClass) ||
        IncorrectVersionException.class.getName().equals(reClass)) {
      LOG.warn("DataNode is shutting down: ", re);
      shutdownDN();
    } else {
      LOG.warn(re);
    }
  }
  
  // this class added by guoleitao, to monitor zk checking namenode ha failover.
  // when failover happend. avatardatanode will set new primaryOfferservice.
  private class NNMonitor implements Runnable, Watcher {
    AvatarDataNode avatardn;
    private AvatarZooKeeperClient zkc;
    private Configuration conf;
    final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean();
    long brDelay = 5*60*1000L;  //when failover, send blockreport in 5 mins (random)

    public NNMonitor(AvatarDataNode AvatarDataNode, Configuration conf) {
      this.avatardn = AvatarDataNode;
      this.conf = conf;
    }

    @Override
    public void run() {
      LOG.info("NNMonitor: thread: " + Thread.currentThread().getName() + " started.");
      zkc = new AvatarZooKeeperClient(conf, this);
      while (this.avatardn.shouldRun  && !this.avatardn.shutdown ) {
        if(zkc.watchAndCheckExists(AvatarZooKeeperClient.activeZnode))
        synchronized (this.clusterHasActiveMaster) {
          try {
            clusterHasActiveMaster.wait();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      LOG.info("NNMonitor: shouldRun is " + this.avatardn.shouldRun + ", shutdown is "
          + this.avatardn.shutdown + " quit "          + Thread.currentThread().getName());
    }

    public void stop() {
      try {
        this.zkc.shutdown();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("NNMonitor: thread: " + Thread.currentThread().getName() + " stoped.");
    }

    @Override
    public void process(WatchedEvent event) {
      LOG.info("NNMonitor: received ZooKeeper Event, " + "type=" + event.getType() + ", "
          + "state=" + event.getState() + ", " + "path=" + event.getPath());

      switch (event.getType()) {
      case NodeCreated: {
        if (event.getPath().equals(AvatarZooKeeperClient.activeZnode)) {
          LOG.info("NNMonitor: recv NodeCreated event, znode = "
              + AvatarZooKeeperClient.activeZnode + ". Start to set primaryOfferservice.");
          handleMasterNodeChange();
        }
        break;
      }
      case NodeDeleted: {
        if (event.getPath().equals(AvatarZooKeeperClient.activeZnode)) {
          LOG.info("NNMonitor: recv NodeDeleted event, znode = "
              + AvatarZooKeeperClient.activeZnode + ". Start to delete primaryOfferservice.");
          handleMasterNodeChange();
        }
        break;
      }
      }
    }

    private void handleMasterNodeChange() {
      // Watch the node and check if it exists.
      if (zkc.watchAndCheckExists(AvatarZooKeeperClient.activeZnode)) {
        if ((null != this.avatardn.offerService1)
            && (this.avatardn.offerService1.isPrimaryService())
            && (!this.avatardn.isPrimaryOfferService(this.avatardn.offerService1))) {
          LOG.info(this.avatardn.offerService1.offerServiceAddress
              + " is primary now. set it as primary offer service.");
          this.avatardn.setPrimaryOfferService(this.avatardn.offerService1);
          synchronized (this.avatardn.offerService1.receivedAndDeletedBlockList) {
            // Need to move acks from the retry list to
            // blockReceivedAndDeleted list to
            // trigger immediate NN notification, since now
            // we are communicating with the primary.
            this.avatardn.offerService1.moveRetryAcks();
          }
          this.avatardn.offerService1.scheduleBlockReport(brDelay); //send block report in 5min
        } else if ((null != this.avatardn.offerService2)
            && (this.avatardn.offerService2.isPrimaryService())
            && (!this.avatardn.isPrimaryOfferService(this.avatardn.offerService2))) {
          LOG.info(this.avatardn.offerService2.offerServiceAddress
              + " is primary now. set it as primary offer service.");
          this.avatardn.setPrimaryOfferService(this.avatardn.offerService2);
          synchronized (this.avatardn.offerService2.receivedAndDeletedBlockList) {
            // Need to move acks from the retry list to
            // blockReceivedAndDeleted list to
            // trigger immediate NN notification, since now
            // we are communicating with the primary.
            this.avatardn.offerService2.moveRetryAcks();
          }
          this.avatardn.offerService2.scheduleBlockReport(brDelay); //send block report in 5min
        } else {
          LOG.info(" TWO offerservice are not primary, what the matter? !");
        }
      } else {
        LOG.info("znode " + AvatarZooKeeperClient.activeZnode
            + " not exist or has no value. set primary offerserice as null.");
        this.avatardn.setPrimaryOfferService(null);
      }
      LOG.debug("Znode has changed, notify the thread watching this.");
      synchronized (clusterHasActiveMaster) {
        // Notify any thread waiting to become the active master
        clusterHasActiveMaster.notifyAll();
      }
    }
  }
  //end add by guoleitao, 20130110
    
  public static void main(String argv[]) {
    try {
      StringUtils.startupShutdownMessage(AvatarDataNode.class, argv, LOG);
      AvatarDataNode avatarnode = createDataNode(argv, null);
      if (avatarnode != null) {
//        avatarnode.waitAndShutdown();
    	  avatarnode.join();
    	  avatarnode.shutdown();
      }
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
