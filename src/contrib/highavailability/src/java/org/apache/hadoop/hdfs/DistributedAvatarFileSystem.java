package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
//import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
//import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
//import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
//import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
//import org.apache.hadoop.hdfs.protocol.LocatedDirectoryListing;
//import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlock;
//import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.ipc.ProtocolSignature;//del by guoleitao
//import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.hadoop.security.token.Token;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class DistributedAvatarFileSystem extends DistributedFileSystem {

  static {
    Configuration.addDefaultResource("avatar-default.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }
  
  CachingAvatarZooKeeperClient zk;
  /*
   * ReadLock is acquired by the clients performing operations WriteLock is
   * acquired when we need to failover and modify the proxy. Read and write
   * because of read and write access to the namenode object.
   */
  ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(true);
  /**
   *  The canonical URI representing the cluster we are connecting to
   *  dfs1.data.xxx.com:9000 for example
   */
  URI logicalName;
  /**
   * The full address of the node in ZooKeeper
   */
  long lastPrimaryUpdate = 0;

  Configuration conf;
  // Wrapper for NameNodeProtocol that handles failover
  FailoverClientProtocol failoverClient;
  // Should DAFS retry write operations on failures or not
  boolean alwaysRetryWrites;
  // Indicates whether subscription model is used for ZK communication 
  boolean watchZK;
  boolean cacheZKData = false;
  // number of milliseconds to wait between successive attempts
  // to initialize standbyFS
//  long standbyFSInitInterval; //del by guoleitao, 20121216
//
//  // time at which we last attempted to initialize standbyFS
//  long lastStandbyFSInit = 0L; //del by guoleitao, 20121216
//
//  // number of milliseconds before we should check if a failover has occurred
//  // (used when making calls to the standby avatar)
//  long standbyFSCheckInterval; //del by guoleitao, 20121216
//
//  // time at which last check for failover was performed
//  long lastStandbyFSCheck = 0L; //del by guoleitao, 20121216
//
//  // number of requests to standbyFS between checks for failover
//  int standbyFSCheckRequestInterval;//del by guoleitao, 20121216 
//
//  // number of requests to standbyFS since last last failover check
//  AtomicInteger standbyFSCheckRequestCount = new AtomicInteger(0);//del by guoleitao, 20121216

  volatile boolean shutdown = false;
  // indicates that the DFS is used instead of DAFS
  volatile boolean fallback = false;

  // We need to keep track of the FS object we used for failover
  DistributedFileSystem failoverFS;

  // a filesystem object that points to the standby avatar
//  StandbyFS standbyFS = null; //the standbyFS is never used in cdh3u4, del all the codes related to standbyFS, guoleitao, 20121216
  // URI of primary and standby avatar
  URI primaryURI;
//  URI standbyURI;//del by guoleitao, 20121216

  // Will try for 30 secs checking with ZK every 10 seconds
  // to see if the failover has happened in pull case
  // and just wait for 30 seconds in watch case
  public static int FAILOVER_CHECK_PERIOD = 10000;
  public static int FAILOVER_RETRIES = 6;
  // Tolerate up to 3 retries connecting to the NameNode
  private static final int FAILURE_RETRY = 3;

  /**
   * HA FileSystem initialization
   */

  @Override
  public URI getUri() {
    return this.logicalName;
  }

  public void initialize(URI name, Configuration conf) throws IOException {
    /*
     * If true clients holds a watch on the znode and acts on events 
     * If false failover is pull based. Client will call zookeeper exists()
     */
    watchZK = conf.getBoolean("fs.ha.zookeeper.watch", true); //default is false, changed by guoleitao 20121218
    /*
     * If false - on Mutable call to the namenode we fail If true we try to make
     * the call go through by resolving conflicts
     */
//    alwaysRetryWrites = conf.getBoolean("fs.ha.retrywrites", false);//del by guoleitao
    alwaysRetryWrites = conf.getBoolean("fs.ha.retrywrites", true); //add by guoleitao
    FAILOVER_CHECK_PERIOD = conf.getInt("fs.ha.failover.check.period", 10000); //default ,10 s
    FAILOVER_RETRIES = conf.getInt("fs.ha.failover.retries", 6); //default 3.

    // The actual name of the filesystem e.g. dfs.data.xxx.com:9000
    this.logicalName = name;
    LOG.info("The logicalName of the filesystem is :"+logicalName);
    this.conf = conf;
    // Create AvatarZooKeeperClient
    Watcher watcher = null;
    if (watchZK) {
      watcher = new ZooKeeperFSWatcher();
    }
    zk = new CachingAvatarZooKeeperClient(conf, watcher);
    cacheZKData = zk.isCacheEnabled();
    // default interval between standbyFS initialization attempts is 10 mins
//    standbyFSInitInterval = conf.getLong("fs.avatar.standbyfs.initinterval",
//                                         10 * 60 * 1000); //del by guoleitao, 20121216
//
//    // default interval between failover checks is 5 min
//    standbyFSCheckInterval = conf.getLong("fs.avatar.standbyfs.checkinterval",
//                                         5 * 60 * 1000);//del by guoleitao, 20121216
//
//    // default interval between failover checks is 5000 requests
//    standbyFSCheckRequestInterval =
//      conf.getInt("fs.avatar.standbyfs.checkrequests", 5000);//del by guoleitao, 20121216

    initUnderlyingFileSystem(false);
  }

  private URI addrToURI(String addrString) throws URISyntaxException {
    if (addrString.startsWith(logicalName.getScheme())) {
      return new URI(addrString);
    } else {
      if (addrString.indexOf(":") == -1) {
        // This is not a valid addr string
        return null;
      }
      String fsHost = addrString.substring(0, addrString.indexOf(":"));
      int port = Integer.parseInt(addrString
                                  .substring(addrString.indexOf(":") + 1));
      return new URI(logicalName.getScheme(),
                     logicalName.getUserInfo(),
                     fsHost, port, logicalName.getPath(),
                     logicalName.getQuery(), logicalName.getFragment());
    }
  }

  //StandbyUI/StandbyFS is never used in cdh3u4, del them all, guoleitao, 20121216
//  private class StandbyFS extends DistributedFileSystem {
//    @Override
//    public URI getUri() {
//      return DistributedAvatarFileSystem.this.logicalName;
//    }
//  }

  /**
   * Try to initialize standbyFS. Must hold writelock to call this method.
   */
//  private void initStandbyFS() {
//    lastStandbyFSInit = System.currentTimeMillis();
//    try {
//      if (standbyFS != null) {
//        standbyFS.close();
//      }
//
//      LOG.info("DAFS initializing standbyFS");
//      LOG.info("DAFS primary=" + primaryURI.toString() +
//               " standby=" + standbyURI.toString());
//      standbyFS = new StandbyFS();
//      //add by guoleitao to fail fast of standbyFS init.
//      //if the standby avatarnode is not on line, then fail fast
////      String standbyAddr = zk.getStandbyAvatarAddress();
////      if(standbyAddr!=null && standbyAddr.length()>0 && addrToURI(standbyAddr).equals(standbyURI)){
//        standbyFS.initialize(standbyURI, conf); 
////        LOG.info("DAFS standbyFS initialized! Have a test of StandbyFS..getDiskStatus().getDfsUsed() = "+standbyFS.getDiskStatus().getDfsUsed());
////      }else{
////    	LOG.warn("DAFS standbyFS can not initialized becaues standbyAddr is null or !=standbURI, standbyAddr is "+standbyAddr+".");
////      }
//      //end add guoleitao
//    } catch (Exception e) {
//      LOG.info("DAFS cannot initialize standbyFS: " +
//               StringUtils.stringifyException(e));
//      standbyFS = null;
//    }
//  }
  //end del

  private boolean initUnderlyingFileSystem(boolean failover) throws IOException {
    try {
      boolean firstAttempt = true;

      while (true) {
        Stat stat = new Stat();
        String primaryAddr = zk.getPrimaryAvatarAddress(logicalName, stat,
            true, firstAttempt);
        LOG.info("DAFS: primaryAddr from zk.getPrimaryAvatarAddress =  "+primaryAddr);
        lastPrimaryUpdate = stat.getMtime();
        primaryURI = addrToURI(primaryAddr);
        LOG.info("DAFS: primaryURI =  "+primaryURI.toString());

        /**
         * comments added by guoleitao
         * fs.default.name0 should be like this hdfs://compute-34-08.local:9000,
         * please do not add "/" in the end like this hdfs://compute-34-08.local:9000/
         * It's the same with "dfs.namenode.dn-address" conf
         * StandbyUI/StandbyFS is never used in cdh3u4, del them all, guoleitao, 20121216
         * */
//        URI uri0 = addrToURI(conf.get("fs.default.name0", ""));
//        LOG.info("in DistributedAvatarFileSystem: uri0 =  "+uri0.toString());
//        // if the uri is null the configuration is broken.
//        // no need to try to initialize a standby fs
//        if (uri0 != null) { //del by guoleitao, 20121216
//          // the standby avatar is whichever one is not the primary
//          // note that standbyFS connects to the datanode port of the standby
//          // avatar
//          // since the client port is not available while in safe mode
//          if (uri0.equals(primaryURI)) {//del by guoleitao, 20121216
//            standbyURI = addrToURI(conf.get("dfs.namenode.dn-address1", ""));
//            LOG.info("in DistributedAvatarFileSystem: uri0=primaryURI, then standbyURI =  "
//                + standbyURI.toString());
//          } else {//del by guoleitao, 20121216
//            standbyURI = addrToURI(conf.get("dfs.namenode.dn-address0", ""));
//            LOG.info("in DistributedAvatarFileSystem: uri0!=primaryURI, then standbyURI =  "
//                + standbyURI.toString());
//          }
//          String standbyNNOnZK = zk.getStandbyAvatarAddress(); //del by guoleitao, 20121216
//          if ((standbyNNOnZK != null) && (standbyNNOnZK.length() >= 0) //del by guoleitao, 20121216
//              && (standbyURI.equals(addrToURI(standbyNNOnZK)))) {
//            LOG.info("standbyNNOnZK = " + standbyNNOnZK + "; start to initStandbyFS.");
//            initStandbyFS();
//          }
//        } else { //del by guoleitao, 20121216
//          LOG.warn("Not initializing standby filesystem because the needed "
//              + "configuration parameters fs.default.name{0|1} are missing.");
//        }
        //end del

        try {
          if (failover) {
            LOG.info("DAFS: It is in failover. failover is "+failover);
            if (failoverFS != null) {
              failoverFS.close();
            }
            failoverFS = new DistributedFileSystem();
            failoverFS.initialize(primaryURI, conf);

            failoverClient.newNameNode(failoverFS.dfs.namenode);

          } else {
            LOG.info("DAFS: It is NOT in failover. failover is "+failover);
            super.initialize(primaryURI, conf);
            failoverClient = new FailoverClientProtocol(this.dfs.namenode);
            this.dfs.namenode = failoverClient;
          }
        } catch (IOException ex) {
          if (firstAttempt && cacheZKData) {
            firstAttempt = false;
            continue;
          } else {
            throw ex;
          }
        }
        LOG.info("DAFS: Initialized new filesystem pointing to " + this.getUri()
            + " with the actual address " + primaryAddr);
        return true;
      }
    } catch (Exception ex) {
      if (failover) {
        // Succeed or die trying
        // Next thread will try to failover again
        failoverFS = null;
        return false;
      } else {
        LOG.error("DAFS: Ecxeption initializing DAFS. " +
      		"Falling back to using DFS instead", ex);
        fallback = true;
        super.initialize(logicalName, conf);
      }
    }
    return true;
  }

  private class FailoverClientProtocol implements ClientProtocol {

    ClientProtocol namenode;

    public FailoverClientProtocol(ClientProtocol namenode) {
      this.namenode = namenode;
    }

    public synchronized void nameNodeDown() {
      namenode = null;
    }

    public synchronized void newNameNode(ClientProtocol namenode) {
      this.namenode = namenode;
    }

    public synchronized boolean isDown() {
      return this.namenode == null;
    }

//    @Override
//    public int getDataTransferProtocolVersion() throws IOException {
//      return (new ImmutableFSCaller<Integer>() {
//        @Override
//        Integer call() throws IOException {
//          return namenode.getDataTransferProtocolVersion();
//        }    
//      }).callFS();
//    }

    @Override
    public void abandonBlock(final Block b, final String src,
        final String holder) throws IOException {
      (new MutableFSCaller<Boolean>() {

        @Override
        Boolean call(int retry) throws IOException {
          namenode.abandonBlock(b, src, holder);
          return true;
        }

      }).callFS();
    }

//    @Override
//    public void abandonFile(final String src,
//        final String holder) throws IOException {
//      (new MutableFSCaller<Boolean>() {
//
//        @Override
//        Boolean call(int retry) throws IOException {
//          namenode.abandonFile(src, holder);
//          return true;
//        }
//
//      }).callFS();
//    } //del by guoleitao

    //not supported in cdh3u4, del, guoleitao
//    @Override
//    public LocatedDirectoryListing getLocatedPartialListing(final String src,
//        final byte[] startAfter) throws IOException {
//      return (new ImmutableFSCaller<LocatedDirectoryListing>() {
//
//        @Override
//        LocatedDirectoryListing call() throws IOException {
//          return namenode.getLocatedPartialListing(src, startAfter);
//        }
//
//      }).callFS();
//    }

  //not support favoredNodes in cdh3u4. this fun is never used. guoleitao
//    @SuppressWarnings("unused")
//	public LocatedBlock addBlock(final String src, final String clientName,
//        final DatanodeInfo[] excludedNodes, final DatanodeInfo[] favoredNodes)
//        throws IOException {
//      return (new MutableFSCaller<LocatedBlock>() {
//        @Override
//        LocatedBlock call(int retries) throws IOException {
//          if (retries > 0) {
////            FileStatus info = namenode.getFileInfo(src);//del
//        	  HdfsFileStatus info = namenode.getFileInfo(src);
//            if (info != null) {
//              LocatedBlocks blocks = namenode.getBlockLocations(src, 0, info
//                .getLen());
//              if (blocks.locatedBlockCount() > 0 ) {
//                LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
//                if (last.getBlockSize() == 0) {
//                  // This one has not been written to
//                  namenode.abandonBlock(last.getBlock(), src, clientName);
//                }
//              }
//            }
//          }
////          return namenode.addBlock(src, clientName, excludedNodes,
////            favoredNodes); //not support favoredNodes in cdh3u4, del
//          return namenode.addBlock(src, clientName, excludedNodes); //add by guoleitao
//        }
//      }).callFS();
//    }

    @Override
    public LocatedBlock addBlock(final String src, final String clientName,
        final DatanodeInfo[] excludedNodes) throws IOException {
      return (new MutableFSCaller<LocatedBlock>() {
        @Override
        LocatedBlock call(int retries) throws IOException {
          if ((null != excludedNodes) && (excludedNodes.length > 0)) LOG
              .info("DAFS:  addBlock has excludeNodes: " + excludedNodes.toString() + " .");
          if (retries > 0) {
//            FileStatus info = namenode.getFileInfo(src); //del
        	HdfsFileStatus info = namenode.getFileInfo(src); //add
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0, info
                  .getLen());
              // If atleast one block exists.
              if (blocks.locatedBlockCount() > 0) {
                LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
                if (last.getBlockSize() == 0) {
                  // This one has not been written to
                  LOG.info("DAFS: addBlock has excludeNodes, the last blocksize is zero, abandon this block.");
                  namenode.abandonBlock(last.getBlock(), src, clientName);
                }
              }
            }
          }
          LOG.info("DAFS: before addBlock(), clientName="+clientName+"; src="+src);
          return namenode.addBlock(src, clientName, excludedNodes);
        }
      }).callFS();
    }

    //multi version and namenode federation are not supported in cdh3u4, del the following. guoleitao
//    @Override
//    public VersionedLocatedBlock addBlockAndFetchVersion(
//        final String src, final String clientName,
//        final DatanodeInfo[] excludedNodes) throws IOException {
//      return (new MutableFSCaller<VersionedLocatedBlock>() {
//        @Override
//        VersionedLocatedBlock call(int retries) throws IOException {
//          if (retries > 0) {
//            FileStatus info = namenode.getFileInfo(src);
//            if (info != null) {
//              LocatedBlocks blocks = namenode.getBlockLocations(src, 0, info
//                  .getLen());
//              // If atleast one block exists.
//              if (blocks.locatedBlockCount() > 0) {
//                LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
//                if (last.getBlockSize() == 0) {
//                  // This one has not been written to
//                  namenode.abandonBlock(last.getBlock(), src, clientName);
//                }
//              }
//            }
//          }
//          return namenode.addBlockAndFetchVersion(src, clientName, excludedNodes);
//        }
//
//      }).callFS();
//    }
//
//    @Override
//    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(
//        final String src, final String clientName,
//        final DatanodeInfo[] excludedNodes) throws IOException {
//      return addBlockAndFetchMetaInfo(
//          src, clientName, excludedNodes, null);
//    }
//
//    @Override
//    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(
//        final String src, final String clientName,
//        final DatanodeInfo[] excludedNodes,
//        final long startPos) throws IOException {
//      return addBlockAndFetchMetaInfo(
//               src, clientName, excludedNodes, null, startPos);
//    }
//    
//    @Override
//    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(final String src,
//        final String clientName, final DatanodeInfo[] excludedNodes,
//        final DatanodeInfo[] favoredNodes)
//        throws IOException {
//      return (new MutableFSCaller<LocatedBlockWithMetaInfo>() {
//        @Override
//        LocatedBlockWithMetaInfo call(int retries) throws IOException {
//          if (retries > 0) {
//            FileStatus info = namenode.getFileInfo(src);
//            if (info != null) {
//              LocatedBlocks blocks = namenode.getBlockLocations(src, 0, info
//                  .getLen());
//              LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
//              if (last.getBlockSize() == 0) {
//                // This one has not been written to
//                namenode.abandonBlock(last.getBlock(), src, clientName);
//              }
//            }
//          }
//          return namenode.addBlockAndFetchMetaInfo(src, clientName,
//              excludedNodes, favoredNodes);
//        }
//
//      }).callFS();
//    }
//
//    @Override
//    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(final String src,
//        final String clientName, final DatanodeInfo[] excludedNodes,
//	final DatanodeInfo[] favoredNodes, final long startPos)
//        throws IOException {
//      return (new MutableFSCaller<LocatedBlockWithMetaInfo>() {
//        @Override
//        LocatedBlockWithMetaInfo call(int retries) throws IOException {
//          if (retries > 0) {
//            FileStatus info = namenode.getFileInfo(src);
//            if (info != null) {
//              LocatedBlocks blocks = namenode.getBlockLocations(src, 0, info
//                  .getLen());
//              LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
//              if (last.getBlockSize() == 0) {
//                // This one has not been written to
//                namenode.abandonBlock(last.getBlock(), src, clientName);
//              }
//            }
//          }
//          return namenode.addBlockAndFetchMetaInfo(src, clientName,
//              excludedNodes, favoredNodes, startPos);
//        }
//
//      }).callFS();
//    }
//end del
    
    @Override
    public LocatedBlock addBlock(final String src, final String clientName)
        throws IOException {
      return (new MutableFSCaller<LocatedBlock>() {
        @Override
        LocatedBlock call(int retries) throws IOException {
          LOG.info("DAFS: addBlock NO excludeNodes");
          if (retries > 0) {
//            FileStatus info = namenode.getFileInfo(src); //del
        	HdfsFileStatus info = namenode.getFileInfo(src); //add
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0, info
                  .getLen());
              // If atleast one block exists.
              if (blocks.locatedBlockCount() > 0) {
                LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
                if (last.getBlockSize() == 0) {
                  LOG.info("DAFS: addBlock NO excludeNodes, the last blocksize is zero, abandon this block.");
                  // This one has not been written to
                  namenode.abandonBlock(last.getBlock(), src, clientName);
                }
              }
            }
          }
          LOG.info("DAFS: addBlock NO excludeNodes, before addBlock(), clientName="+clientName+"; src="+src);
          return namenode.addBlock(src, clientName);
        }

      }).callFS();
    }

    @Override
    public LocatedBlock append(final String src, final String clientName)
        throws IOException {
      return (new MutableFSCaller<LocatedBlock>() {
        @Override
        LocatedBlock call(int retries) throws IOException {
          LOG.info("DAFS: append");
          if (retries > 0) {
            namenode.complete(src, clientName);
          }
          return namenode.append(src, clientName);
        }

      }).callFS();
    }

    //append with location is not support in cdh3u4, del
//    @Override
//    public LocatedBlockWithMetaInfo appendAndFetchMetaInfo(final String src,
//        final String clientName) throws IOException {
//      return (new MutableFSCaller<LocatedBlockWithMetaInfo>() {
//        @Override
//        LocatedBlockWithMetaInfo call(int retries) throws IOException {
//          if (retries > 0) {
//            namenode.complete(src, clientName);
//          }
//          return namenode.appendAndFetchMetaInfo(src, clientName);
//        }
//      }).callFS();
//    }

    @Override
    public boolean complete(final String src, final String clientName)
        throws IOException {
      // Treating this as Immutable even though it changes metadata
      // but the complete called on the file should result in completed file
      return (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          if (r > 0) {
            try {
            	LOG.info("DAFS: complete(), src="+ src +" ; clientName="+clientName);
              return namenode.complete(src, clientName);
            } catch (IOException ex) {
              if (namenode.getFileInfo(src) != null) {
                // This might mean that we closed that file
                // which is why namenode can no longer find it
                // in the list of UnderConstruction
                if (ex.getMessage()
                    .contains("Could not complete write to file")) {
                  // We guess that we closed this file before because of the
                  // nature of exception
                  return true;
                }
              }
              throw ex;
            }
            
          }
          LOG.info("DAFS: complete(), before calling to NameNode.");
          return namenode.complete(src, clientName);
        }
      }).callFS();
    }

// this fun will attach the filelen for namenode to verify the complete of file.
// this fun is not supported in cdh3u4 so far, del for now. guoleitao
//    @Override
//    public boolean complete(final String src, final String clientName, final long fileLen)
//        throws IOException {
//      // Treating this as Immutable even though it changes metadata
//      // but the complete called on the file should result in completed file
//      return (new MutableFSCaller<Boolean>() {
//        Boolean call(int r) throws IOException {
//          if (r > 0) {
//            try {
//              return namenode.complete(src, clientName, fileLen);
//            } catch (IOException ex) {
//              if (namenode.getFileInfo(src) != null) {
//                // This might mean that we closed that file
//                // which is why namenode can no longer find it
//                // in the list of UnderConstruction
//                if (ex.getMessage()
//                    .contains("Could not complete write to file")) {
//                  // We guess that we closed this file before because of the
//                  // nature of exception
//                  return true;
//                }
//              }
//              throw ex;
//            }
//          }
//          return namenode.complete(src, clientName, fileLen);
//        }
//      }).callFS();
//    }

    @Override
    public void create(final String src, final FsPermission masked,
        final String clientName, final boolean overwrite,
        final short replication, final long blockSize) throws IOException {
     (new MutableFSCaller<Boolean>() {
       @Override
       Boolean call(int retries) throws IOException {
         namenode.create(src, masked, clientName, overwrite,
            replication, blockSize);
         return true;
       }
     }).callFS();
   }

    @Override
    public void create(final String src, final FsPermission masked,
        final String clientName, final boolean overwrite,
        final boolean createParent,
        final short replication, final long blockSize) throws IOException {
      (new MutableFSCaller<Boolean>() {
        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            // This I am not sure about, because of lease holder I can tell if
            // it
            // was me or not with a high level of certainty
//            FileStatus stat = namenode.getFileInfo(src); //del
            HdfsFileStatus stat = namenode.getFileInfo(src); //add
            if (stat != null && !overwrite) {
              /*
               * Since the file exists already we need to perform a number of
               * checks to see if it was created by us before the failover
               */

              if (stat.getBlockSize() == blockSize
                  && stat.getReplication() == replication && stat.getLen() == 0
                  && stat.getPermission().equals(masked)) {
                // The file has not been written to and it looks exactly like
                // the file we were trying to create. Last check:
                // call create again and then parse the exception.
                // Two cases:
                // it was the same client who created the old file
                // or it was created by someone else - fail
                try {
                  namenode.create(src, masked, clientName, overwrite,
                      createParent, replication, blockSize);
                } catch (AlreadyBeingCreatedException aex) {
                  if (aex.getMessage().contains(
                      "current leaseholder is trying to recreate file")) {
                    namenode.delete(src, false);
                  } else {
                    throw aex;
                  }
                }
              }
            }
          }
          namenode.create(src, masked, clientName, overwrite, createParent, replication,
              blockSize);
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean delete(final String src, final boolean recursive)
        throws IOException {
      return (new MutableFSCaller<Boolean>() {
        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            namenode.delete(src, recursive);
            return true;
          }
          return namenode.delete(src, recursive);
        }

      }).callFS();
    }

    @Override
    public boolean delete(final String src) throws IOException {
      return (new MutableFSCaller<Boolean>() {
        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            namenode.delete(src);
            return true;
          }
          return namenode.delete(src);
        }

      }).callFS();
    }

    @Override
    public UpgradeStatusReport distributedUpgradeProgress(
        final UpgradeAction action) throws IOException {
      return (new MutableFSCaller<UpgradeStatusReport>() {
        UpgradeStatusReport call(int r) throws IOException {
          return namenode.distributedUpgradeProgress(action);
        }
      }).callFS();
    }

    @Override
    public void finalizeUpgrade() throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int retry) throws IOException {
          namenode.finalizeUpgrade();
          return true;
        }
      }).callFS();

    }

    @Override
    public void fsync(final String src, final String client) throws IOException {
      // TODO Is it Mutable or Immutable
      (new ImmutableFSCaller<Boolean>() {

        @Override
        Boolean call() throws IOException {
          namenode.fsync(src, client);
          return true;
        }

      }).callFS();
    }

    @Override
    public LocatedBlocks getBlockLocations(final String src, final long offset,
        final long length) throws IOException {
      // TODO Make it cache values as per Dhruba's suggestion
      return (new ImmutableFSCaller<LocatedBlocks>() {
        LocatedBlocks call() throws IOException {
          return namenode.getBlockLocations(src, offset, length);
        }
      }).callFS();
    }

    // incompatible version is not support in cdh3u4, del, guoleitao
//    @Override
//    public VersionedLocatedBlocks open(final String src, final long offset,
//        final long length) throws IOException {
//      // TODO Make it cache values as per Dhruba's suggestion
//      return (new ImmutableFSCaller<VersionedLocatedBlocks>() {
//        VersionedLocatedBlocks call() throws IOException {
//          return namenode.open(src, offset, length);
//        }
//      }).callFS();
//    }
//
//    @Override
//    public LocatedBlocksWithMetaInfo openAndFetchMetaInfo(final String src, final long offset,
//        final long length) throws IOException {
//      // TODO Make it cache values as per Dhruba's suggestion
//      return (new ImmutableFSCaller<LocatedBlocksWithMetaInfo>() {
//        LocatedBlocksWithMetaInfo call() throws IOException {
//          return namenode.openAndFetchMetaInfo(src, offset, length);
//        }
//      }).callFS();
//    }
    //end del

    @Override
    public ContentSummary getContentSummary(final String src) throws IOException {
      return (new ImmutableFSCaller<ContentSummary>() {
        ContentSummary call() throws IOException {
          return namenode.getContentSummary(src);
        }
      }).callFS();
    }

    //seems only invoked by DFSClient, no use for now, del, guoleitao
//    @Deprecated @Override
//    public FileStatus[] getCorruptFiles() throws AccessControlException,
//        IOException {
//      return (new ImmutableFSCaller<FileStatus[]>() {
//        FileStatus[] call() throws IOException {
//          return namenode.getCorruptFiles();
//        }
//      }).callFS();
//    }
//    
    //seems only invoked by DFSClient, no use for now, del, guoleitao
//    @Override
//    public CorruptFileBlocks
//      listCorruptFileBlocks(final String path, final String cookie)
//      throws IOException {
//      return (new ImmutableFSCaller<CorruptFileBlocks> () {
//                CorruptFileBlocks call() 
//                  throws IOException {
//                  return namenode.listCorruptFileBlocks(path, cookie);
//                }
//              }).callFS();
//    }

    @Override
    public DatanodeInfo[] getDatanodeReport(final DatanodeReportType type)
        throws IOException {
      return (new ImmutableFSCaller<DatanodeInfo[]>() {
        DatanodeInfo[] call() throws IOException {
          return namenode.getDatanodeReport(type);
        }
      }).callFS();
    }

    //add by guoleitao
    @Override
    public HdfsFileStatus getFileInfo(final String src) throws IOException {
      return (new ImmutableFSCaller<HdfsFileStatus>() {
        HdfsFileStatus call() throws IOException {
          return namenode.getFileInfo(src);
        }
      }).callFS();
    }    
    //end add
    
    //not support in cdh3u4
//    @Override
//    public FileStatus getFileInfo(final String src) throws IOException {
//      return (new ImmutableFSCaller<FileStatus>() {
//        FileStatus call() throws IOException {
//          return namenode.getFileInfo(src);
//        }
//      }).callFS();
//    }

    //not support in cdh3u4
//    @Override
//    public HdfsFileStatus getHdfsFileInfo(final String src) throws IOException {
//      return (new ImmutableFSCaller<HdfsFileStatus>() {
//        HdfsFileStatus call() throws IOException {
//          return namenode.getHdfsFileInfo(src);
//        }
//      }).callFS();
//    }

    //not support in cdh3u4, del
//    @Override
//    public HdfsFileStatus[] getHdfsListing(final String src) throws IOException {
//      return (new ImmutableFSCaller<HdfsFileStatus[]>() {
//        HdfsFileStatus[] call() throws IOException {
//          return namenode.getHdfsListing(src);
//        }
//      }).callFS();
//    }
//
//    @Override
//    public FileStatus[] getListing(final String src) throws IOException {
//      return (new ImmutableFSCaller<FileStatus[]>() {
//        FileStatus[] call() throws IOException {
//          return namenode.getListing(src);
//        }
//      }).callFS();
//    }
//
//    public DirectoryListing getPartialListing(final String src,
//        final byte[] startAfter) throws IOException {
//      return (new ImmutableFSCaller<DirectoryListing>() {
//        DirectoryListing call() throws IOException {
//          return namenode.getPartialListing(src, startAfter);
//        }
//      }).callFS();
//    }

    @Override
    public long getPreferredBlockSize(final String filename) throws IOException {
      return (new ImmutableFSCaller<Long>() {
        Long call() throws IOException {
          return namenode.getPreferredBlockSize(filename);
        }
      }).callFS();
    }

    @Override
    public long[] getStats() throws IOException {
      return (new ImmutableFSCaller<long[]>() {
        long[] call() throws IOException {
          return namenode.getStats();
        }
      }).callFS();
    }

    @Override
    public void metaSave(final String filename) throws IOException {
      (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.metaSave(filename);
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean mkdirs(final String src, final FsPermission masked)
        throws IOException {
      return (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          return namenode.mkdirs(src, masked);
        }
      }).callFS();
    }

    @Override
    public void refreshNodes() throws IOException {
      (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.refreshNodes();
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean rename(final String src, final String dst) throws IOException {
      return (new MutableFSCaller<Boolean>() {

        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            /*
             * Because of the organization of the code in the NameNode if the
             * source is still there then the rename did not happen
             * 
             * If it doesn't exist then if the rename happened, the dst exists
             * otherwise rename did not happen because there was an error return
             * false
             * 
             * This is of course a subject to races between clients but with
             * certain assumptions about a system we can make the call succeed
             * on failover
             */
            if (namenode.getFileInfo(src) != null)
              return namenode.rename(src, dst);
            return namenode.getFileInfo(dst) != null;
          }
          return namenode.rename(src, dst);
        }

      }).callFS();
    }

    @Override
    public void renewLease(final String clientName) throws IOException {
      // Treating this as immutable
      (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.renewLease(clientName);
          return true;
        }
      }).callFS();
    }

    @Override
    public void reportBadBlocks(final LocatedBlock[] blocks) throws IOException {
      // TODO this might be a good place to send it to both namenodes
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.reportBadBlocks(blocks);
          return true;
        }
      }).callFS();
    }

    @Override
    public void saveNamespace() throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.saveNamespace();
          return true;
        }
      }).callFS();
    }

    // force and uncompressed are not support in cdh3u4, del this fuc. guoleitao
//    @Override
//    public void saveNamespace(final boolean force, final boolean uncompressed) throws IOException {
//      (new MutableFSCaller<Boolean>() {
//        Boolean call(int r) throws IOException {
//          namenode.saveNamespace(force, uncompressed);
//          return true;
//        }
//      }).callFS();
//    }

    @Override
    public void setOwner(final String src, final String username,
        final String groupname) throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.setOwner(src, username, groupname);
          return true;
        }
      }).callFS();
    }

    @Override
    public void setPermission(final String src, final FsPermission permission)
        throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.setPermission(src, permission);
          return true;
        }
      }).callFS();
    }

    @Override
    public void setQuota(final String path, final long namespaceQuota,
        final long diskspaceQuota) throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int retry) throws IOException {
          namenode.setQuota(path, namespaceQuota, diskspaceQuota);
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean setReplication(final String src, final short replication)
        throws IOException {
      return (new MutableFSCaller<Boolean>() {
        Boolean call(int retry) throws IOException {
          return namenode.setReplication(src, replication);
        }
      }).callFS();
    }

    @Override
    public boolean setSafeMode(final SafeModeAction action) throws IOException {
      return (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          return namenode.setSafeMode(action);
        }
      }).callFS();
    }

    @Override
    public void setTimes(final String src, final long mtime, final long atime)
        throws IOException {
      (new MutableFSCaller<Boolean>() {

        @Override
        Boolean call(int retry) throws IOException {
          namenode.setTimes(src, mtime, atime);
          return true;
        }

      }).callFS();
    }

    //not support in cdh3u4, del
//    @Override
//    @Deprecated
//    public void concat(final String trg, final String[] srcs
//        ) throws IOException {
//      concat(trg, srcs, true);
//    }
//    
//    @Override
//    public void concat(final String trg, final String[] srcs,
//        final boolean restricted) throws IOException {
//      (new MutableFSCaller<Boolean>() {
//        Boolean call(int r) throws IOException {
//          namenode.concat(trg, srcs, restricted);
//          return true;
//        }
//      }).callFS();
//    }
    
    @Override
    public long getProtocolVersion(final String protocol,
//        final long clientVersion) throws VersionIncompatible, IOException { //del
          final long clientVersion) throws IOException { //add
      return (new ImmutableFSCaller<Long>() {

        @Override
        Long call() throws IOException {
          return namenode.getProtocolVersion(protocol, clientVersion);
        }

      }).callFS();
    }

    //del by guoleitao
//    @Override
//    public ProtocolSignature getProtocolSignature(final String protocol,
//        final long clientVersion, final int clientMethodsHash) throws IOException {
//      return (new ImmutableFSCaller<ProtocolSignature>() {
//
//        @Override
//        ProtocolSignature call() throws IOException {
//          return namenode.getProtocolSignature(
//              protocol, clientVersion, clientMethodsHash);
//        }
//
//      }).callFS();
//    }

    @Override
//    public void recoverLease(final String src, final String clientName)	//del
    public boolean recoverLease(final String src, final String clientName)	//add by guoleitao
    throws IOException {
      // Treating this as immutable
      (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.recoverLease(src, clientName);
          return true;
        }
      }).callFS();
      return false; //add by guoleitao
    }
    
    //the following add by guoleitao

	@Override
	public DirectoryListing getListing(final String src, final byte[] startAfter)
			throws IOException {
	      return (new ImmutableFSCaller<DirectoryListing>() {
	    	  DirectoryListing call() throws IOException {
	            return namenode.getListing(src,startAfter);
	          }
	        }).callFS();
	      }

	@Override
	public Token<DelegationTokenIdentifier> getDelegationToken(final Text renewer)
			throws IOException {
	      return (new ImmutableFSCaller<Token<DelegationTokenIdentifier>>() {
	    	  Token<DelegationTokenIdentifier> call() throws IOException {
	            return namenode.getDelegationToken(renewer);
	          }
	        }).callFS();
	      }

	@Override
		public long renewDelegationToken(
				final Token<DelegationTokenIdentifier> token)
				throws IOException {
			return (new MutableFSCaller<Long>() {
				@Override
				Long call(int retries) throws IOException {
					return namenode.renewDelegationToken(token);
				}
			}).callFS();
		}

	@Override
	public void cancelDelegationToken(final Token<DelegationTokenIdentifier> token)
			throws IOException {
	      (new ImmutableFSCaller<Boolean>() {
	          Boolean call() throws IOException {
	            namenode.cancelDelegationToken(token);
	            return true;
	          }
	        }).callFS();
	}
//end add guoleitao
	
    //not support in cdh3u4, del for now. guoleitao
//    @Override
//    public boolean closeRecoverLease(final String src, final String clientName)
//    throws IOException {
//      // Treating this as immutable
//      return (new ImmutableFSCaller<Boolean>() {
//        Boolean call() throws IOException {
//          return namenode.closeRecoverLease(src, clientName, false);
//        }
//      }).callFS();
//    }
//
//    @Override
//    public boolean closeRecoverLease(final String src, final String clientName,
//                                     final boolean discardLastBlock)
//       throws IOException {
//      // Treating this as immutable
//      return (new ImmutableFSCaller<Boolean>() {
//        Boolean call() throws IOException {
//          return namenode.closeRecoverLease(src, clientName, discardLastBlock);
//        }
//      }).callFS();
//    }     //end del
  }


  private boolean shouldHandleException(IOException ex) {
    if (ex.getMessage().contains("java.io.EOFException")) {
      return true;
    }
    return ex.getMessage().toLowerCase().contains("connection");
  }

  /**
   * @return true if a failover has happened, false otherwise
   * requires write lock
   */
  private boolean zkCheckFailover() {
    try {
      long registrationTime = zk.getPrimaryRegistrationTime(logicalName);
      LOG.info("DAFS: File is in ZK");
      LOG.info("DAFS: Checking mod time: " + registrationTime + 
                " > " + lastPrimaryUpdate);
      if (registrationTime > lastPrimaryUpdate) {
        // Failover has happened happened already
        failoverClient.nameNodeDown();
        return true;
      }
    } catch (Exception x) {
      // just swallow for now
      LOG.error(x);
    }
    return false;
  }

  private void handleFailure(IOException ex, int failures) throws IOException {
    LOG.info("DAFS: Handle failure", ex);
    // Check if the exception was thrown by the network stack
    if (shutdown || !shouldHandleException(ex)) {
      throw ex;
    }

    if (failures > FAILURE_RETRY) {
      throw ex;
    }
    try {
      // This might've happened because we are failing over
      if (!watchZK) {
        LOG.info("DAFS: Not watching ZK, so checking explicitly");
        // Check with zookeeper
        fsLock.readLock().unlock();
        fsLock.writeLock().lock();
        boolean failover = zkCheckFailover();
        fsLock.writeLock().unlock();
        fsLock.readLock().lock();
        if (failover) {
          return;
        }
      }
      Thread.sleep(1000);
    } catch (InterruptedException iex) {
      LOG.error("DAFS: Interrupted while waiting for a failover", iex);
      Thread.currentThread().interrupt();
    }

  }

  @Override
  public void close() throws IOException {
    shutdown = true;
    if (fallback) {
      // no need to lock resources
      super.close();
      return;
    }
    readLock();
    try {
      super.close();
      if (failoverFS != null) {
        failoverFS.close();
      }
      //StandbyUI/StandbyFS is never used in cdh3u4, del them all, guoleitao, 20121216
//      if (standbyFS != null) { 
//        standbyFS.close();
//      }
      //end del
      try {
        zk.shutdown();
      } catch (InterruptedException e) {
        LOG.error("DAFS: Error shutting down ZooKeeper client", e);
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * ZooKeeper communication
   */

  private class ZooKeeperFSWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      /**
       * This is completely inaccurate by now since we
       * switched from deletion and recreation of the node
       * to updating the node information.
       * I am commenting it out for now. Will revisit once we
       * decide to go to the watchers approach.
       */
      //the following uncommented by guoleitao. DAFS should update to new namenode when failover happened.
      if (Event.EventType.NodeCreated == event.getType()
          && event.getPath().equals(AvatarZooKeeperClient.activeZnode)) {
        LOG.info("DAFS: ZooKeeperFSWatcher get NodeCreated event, znode = "+AvatarZooKeeperClient.activeZnode+". re-init the Filesytem.");
        fsLock.writeLock().lock();
        try {
          initUnderlyingFileSystem(true);
          LOG.info("DAFS: initUnderlyingFileSystem done when get NodeCreated event.");
        } catch (IOException ex) {
          LOG.error("Error initializing fs", ex);
        } finally {
          fsLock.writeLock().unlock();
        }
        return;
      }
      if (Event.EventType.NodeDeleted == event.getType()
          && event.getPath().equals(AvatarZooKeeperClient.activeZnode)) {
        LOG.info("DAFS: ZooKeeperFSWatcher get NodeDeleted event, znode = "+AvatarZooKeeperClient.activeZnode+". re-init the Filesytem.");
        fsLock.writeLock().lock();
        LOG.info("DAFS: set namenode down."); //add by guoleitao
        failoverClient.nameNodeDown();        
        try {
          // Subscribe for changes
          if (zk.getNodeStats(AvatarZooKeeperClient.activeZnode) != null) {
            // Failover already happened - initialize
            initUnderlyingFileSystem(true);
            LOG.info("DAFS: initUnderlyingFileSystem done when get NodeDeleted event.");
          }
        } catch (Exception iex) {
          LOG.error(iex);
        } finally {
          fsLock.writeLock().unlock();
        }
      }
      //end uncommented by guoleitao, 20121218
    }
  }

  private void readUnlock() {
    fsLock.readLock().unlock();
  }

  private void readLock() throws IOException {
    for (int i = 0; i < FAILOVER_RETRIES; i++) {
      fsLock.readLock().lock();

      if (failoverClient.isDown()) {
        // This means the underlying filesystem is not initialized
        // and there is no way to make a call
        // Failover might be in progress, so wait for it
        // Since we do not want to miss the notification on failoverMonitor
        fsLock.readLock().unlock();
        try {
          boolean failedOver = false;
          fsLock.writeLock().lock();
          if (!watchZK && failoverClient.isDown()) {
            LOG.debug("DAFS: No Watch ZK Failover");
            // We are in pull failover mode where clients are asking ZK
            // if the failover is over instead of ZK telling watchers
            // however another thread in this FS Instance could've done
            // the failover for us.
            try {
              failedOver = initUnderlyingFileSystem(true);
            } catch (Exception ex) {
              // Just swallow exception since we are retrying in any event
            }
          }
          fsLock.writeLock().unlock();
          if (!failedOver)
            Thread.sleep(FAILOVER_CHECK_PERIOD);
        } catch (InterruptedException ex) {
          LOG.error("DAFS: Got interrupted waiting for failover", ex);
          Thread.currentThread().interrupt();
        }

      } else {
        // The client is up and we are holding a readlock.
        return;
      }
    }
    // We retried FAILOVER_RETRIES times with no luck - fail the call
    throw new IOException("DAFS: No FileSystem for " + logicalName);
  }

  /**
   * File System implementation
   */


  private abstract class ImmutableFSCaller<T> {
    
    abstract T call() throws IOException;
    
    public T callFS() throws IOException {
      int failures = 0;
      while (true) {
        readLock();
        try {
          return this.call();
        } catch (IOException ex) {
          handleFailure(ex, failures);
          failures++;
        } finally {
          readUnlock();
        }
      }
    }
  }

  private abstract class MutableFSCaller<T> {

    abstract T call(int retry) throws IOException;
    
    public T callFS() throws IOException {
      int retries = 0;
      while (true) {
        readLock();
        try {
          return this.call(retries);
        } catch (IOException ex) {
          if (!alwaysRetryWrites)
            throw ex;
          handleFailure(ex, retries);
          retries++;
        } finally {
          readUnlock();
        }
      }
    }
  }

  //the following codes never called by cdh3u4, del them all. guoleitao, 20121216
  
//  /**
//   * Used to direct a call either to the standbyFS or to the underlying DFS.
//   */
//  private abstract class StandbyCaller<T> {
//    abstract T call(DistributedFileSystem fs) throws IOException;
//
//    private T callPrimary() throws IOException {
//      return call(DistributedAvatarFileSystem.this);
//    }
//
//    private T callStandby() throws IOException {
//      boolean primaryCalled = false;
//      try {
//        // grab the read lock but don't check for failover yet
//        fsLock.readLock().lock();
//
//        if (System.currentTimeMillis() >
//            lastStandbyFSCheck + standbyFSCheckInterval ||
//            standbyFSCheckRequestCount.get() >=
//            standbyFSCheckRequestInterval) {
//          // check if a failover has happened
//          LOG.debug("DAFS checking for failover time=" +
//                    System.currentTimeMillis() +
//                    " last=" + lastStandbyFSCheck +
//                    " t_interval=" + standbyFSCheckInterval +
//                    " count=" + (standbyFSCheckRequestCount.get()) +
//                    " r_interval=" + standbyFSCheckRequestInterval);
//
//          // release read lock, grab write lock
//          fsLock.readLock().unlock();
//          fsLock.writeLock().lock();
//          boolean failover = zkCheckFailover();
//          if (failover) {
//            LOG.info("DAFS failover has happened");
//            failoverClient.nameNodeDown();
//          } else {
//            LOG.debug("DAFS failover has not happened");
//          }
//        
//          standbyFSCheckRequestCount.set(0);
//          lastStandbyFSCheck = System.currentTimeMillis();
//
//          // release write lock
//          fsLock.writeLock().unlock();
//
//          // now check for failover
//          readLock();
//        } else if (standbyFS == null && (System.currentTimeMillis() >
//                                         lastStandbyFSInit +
//                                         standbyFSInitInterval)) {
//          // try to initialize standbyFS
//
//          // release read lock, grab write lock
//          fsLock.readLock().unlock();
//          fsLock.writeLock().lock();
//          initStandbyFS();
//
//          fsLock.writeLock().unlock();
//          fsLock.readLock().lock();
//        }
//
//        standbyFSCheckRequestCount.incrementAndGet();
//
//        if (standbyFS == null) {
//          // if there is still no standbyFS, use the primary
//          LOG.info("DAFS Standby avatar not available, using primary.");
//          primaryCalled = true;
//          fsLock.readLock().unlock();
//          return callPrimary();
//        }
//        return call(standbyFS);
//      } catch (FileNotFoundException fe) {
//        throw fe;
//      } catch (IOException ie) {
//        if (primaryCalled) {
//          throw ie;
//        } else {
//          LOG.error("DAFS Request to standby avatar failed, trying primary.\n" +
//                    "Standby exception:\n" +
//                    StringUtils.stringifyException(ie));
//          primaryCalled = true;
//          fsLock.readLock().unlock();
//          return callPrimary();
//        }
//      } finally {
//        if (!primaryCalled) {
//          fsLock.readLock().unlock();
//        }
//      }
//    }
//
//    T callFS(boolean useStandby) throws IOException {
//      if (!useStandby) {
//        LOG.debug("DAFS using primary");
//        return callPrimary();
//      } else {
//        LOG.debug("DAFS using standby");
//        return callStandby();
//      }
//    }
//  }
//
//  /**
//   * Return the stat information about a file.
//   * @param f path
//   * @param useStandby flag indicating whether to read from standby avatar
//   * @throws FileNotFoundException if the file does not exist.
//   */
//  public FileStatus getFileStatus(final Path f, final boolean useStandby)
//    throws IOException {
//    return new StandbyCaller<FileStatus>() {
//      @Override
//      FileStatus call(DistributedFileSystem fs) throws IOException {
//        return fs.getFileStatus(f);
//      }
//    }.callFS(useStandby);
//  }
//
//  /**
//   * List files in a directory.
//   * @param f path
//   * @param useStandby flag indicating whether to read from standby avatar
//   * @throws FileNotFoundException if the file does not exist.
//   */
//  public FileStatus[] listStatus(final Path f, final boolean useStandby)
//    throws IOException {
//    return new StandbyCaller<FileStatus[]>() {
//      @Override
//      FileStatus[] call(DistributedFileSystem fs) throws IOException {
//        return fs.listStatus(f);
//      }
//    }.callFS(useStandby);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  public ContentSummary getContentSummary(final Path f,
//                                          final boolean useStandby)
//    throws IOException {
//    return new StandbyCaller<ContentSummary>() {
//      @Override
//      ContentSummary call(DistributedFileSystem fs) throws IOException {
//        return fs.getContentSummary(f);
//      }
//    }.callFS(useStandby);
//  }
//
//  /**
//   * {@inheritDoc}
//   *
//   * This will only work if the standby avatar is
//   * set up to populate its underreplicated
//   * block queues while still in safe mode.
//   */
////  this fun only invoked by DFSClient, del it. guoleitao
////  public RemoteIterator<Path> listCorruptFileBlocks(final Path path,
////                                                    final boolean useStandby)
////    throws IOException {
////    return new StandbyCaller<RemoteIterator<Path>>() {
////      @Override
////      RemoteIterator<Path> call(DistributedFileSystem fs) throws IOException {
////        return fs.listCorruptFileBlocks(path);
////      }
////    }.callFS(useStandby);
////  }
//
//  
//  /**
//   * Return statistics for each datanode.
//   * @return array of data node reports
//   * @throw IOException
//   */
//  public DatanodeInfo[] getDataNodeStats(final boolean useStandby)
//    throws IOException {
//    return new StandbyCaller<DatanodeInfo[]>() {
//      @Override
//      DatanodeInfo[] call(DistributedFileSystem fs) throws IOException {
//        return fs.getDataNodeStats();
//      }
//    }.callFS(useStandby);
//  }
//
//  /**
//   * Return statistics for datanodes that are alive.
//   * @return array of data node reports
//   * @throw IOException
//   */
//  public DatanodeInfo[] getLiveDataNodeStats(final boolean useStandby)
//    throws IOException {
//    return new StandbyCaller<DatanodeInfo[]>() {
//      @Override
//      DatanodeInfo[] call(DistributedFileSystem fs) throws IOException {
//        return fs.getLiveDataNodeStats();
//      }
//    }.callFS(useStandby);
//  } //not support in cdh3u4, del, guoleitao

  //end del, guoleitao, 20121216
}
