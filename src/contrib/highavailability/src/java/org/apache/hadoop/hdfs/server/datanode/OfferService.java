/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.apache.hadoop.ipc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.AvatarZooKeeperClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReceivedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BlockRecord;
// import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode.ServicePair; //del by guoleitao
// import org.apache.hadoop.hdfs.server.datanode.DataNode.KeepAliveHeartbeater;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.zookeeper.data.Stat;

public class OfferService implements Runnable {

  public static final Log LOG = LogFactory.getLog(OfferService.class.getName());

  // private final static int BACKOFF_DELAY = 5 * 60 * 1000; //del
  private final static int BACKOFF_DELAY = 1 * 1000; // add

  long lastHeartbeat = 0;
  volatile boolean shouldRun = true;
  long lastBlockReport = 0;
  long lastDeletedReport = 0;
  boolean resetBlockReportTime = true;
  long blockReceivedRetryInterval;
  int reportsSinceRegister;
  AvatarDataNode anode;
  DatanodeProtocol namenode;
  AvatarProtocol avatarnode;
  InetSocketAddress namenodeAddress;
  InetSocketAddress avatarnodeAddress;
  String offerServiceAddress = null;
  DatanodeRegistration nsRegistration = null;
  FSDatasetInterface data;
  DataNodeMetrics myMetrics;
  ScheduledExecutorService keepAliveSender = null;
  ScheduledFuture keepAliveRun = null;
  private static final Random R = new Random();
  private int backlogSize; // if we accumulate this many blockReceived, then it is time
                           // to send a block report. Otherwise the receivedBlockList
                           // might exceed our Heap size.
  private LinkedList<Block> receivedAndDeletedRetryBlockList = new LinkedList<Block>();
  public LinkedList<Block> receivedAndDeletedBlockList = new LinkedList<Block>();

  private int pendingReceivedRequests = 0;

  private long lastBlockReceivedFailed = 0;
  // private ServicePair servicePair;

  private boolean shouldBackoff = false;
  private boolean firstBlockReportSent = false;
  AvatarZooKeeperClient zkClient = null;

  /**
   * Offer service to the specified namenode
   */
  // public OfferService(AvatarDataNode anode, ServicePair servicePair,
  // DatanodeProtocol namenode, InetSocketAddress namenodeAddress,
  // AvatarProtocol avatarnode, InetSocketAddress avatarnodeAddress) { //del by guoleitao
  public OfferService(AvatarDataNode anode, DatanodeProtocol namenode,
      InetSocketAddress namenodeAddress, AvatarProtocol avatarnode,
      InetSocketAddress avatarnodeAddress) {
    this.anode = anode;
    // this.servicePair = servicePair;
    this.namenode = namenode;
    this.avatarnode = avatarnode;
    this.namenodeAddress = namenodeAddress;
    this.avatarnodeAddress = avatarnodeAddress;
    if (this.namenodeAddress != null) {
      offerServiceAddress = this.namenodeAddress.getHostName() + ":"
          + this.namenodeAddress.getPort();
      LOG.info(offerServiceAddress + ": " + "AvatarDataNode OfferService: namenodeAddress="
          + namenodeAddress);

    } else {
      LOG.info("AvatarDataNode OfferService: this.namenodeAddress = null");
    }
    // nsRegistration = servicePair.nsRegistration;
    this.nsRegistration = this.anode.dnRegistration;
    data = anode.data;
    myMetrics = anode.myMetrics;
    scheduleBlockReport(anode.initialBlockReportDelay);
    backlogSize = anode.getConf().getInt("dfs.datanode.blockreceived.backlog", 10000);
    blockReceivedRetryInterval = anode.getConf().getInt(
      "dfs.datanode.blockreceived.retry.internval", 10000);
  }

  public void stop() {
    shouldRun = false;
    if (keepAliveRun != null) {
      keepAliveRun.cancel(false);
    }
    if (keepAliveSender != null) {
      keepAliveSender.shutdownNow();
    }
    if (zkClient != null) { // added by guoleitao
      try {
        zkClient.shutdown();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }// end add
  }

  public boolean isPrimaryService() {
    if (null == zkClient) zkClient = new AvatarZooKeeperClient(this.anode.getConf(), null);
    try {
      Stat stat = new Stat();
      // String actual = servicePair.zkClient.getPrimaryAvatarAddress(
      // servicePair.defaultAddr, stat, true); //del by guoleitao
      String defaultAddr = this.anode.nameAddr1.getHostName() + ":"
          + this.anode.nameAddr1.getPort();// add by guoleitao
      LOG.info(offerServiceAddress + ": " + "AvatarDataNode OfferService: defaultAddr="
          + defaultAddr);
      String actual = null;
      if (this.anode != null) {
        // actual = this.anode.zkClient.getPrimaryAvatarAddress(
        actual = zkClient.getPrimaryAvatarAddress(defaultAddr, stat, true); // add by guoleitao
        LOG.info(offerServiceAddress + ": " + "AvatarDataNode OfferService: actual=" + actual);
      } else {
        LOG.info(offerServiceAddress + ": " + "AvatarDataNode OfferService: this.anode = null");
      }
      LOG.info(offerServiceAddress + ": " + "AvatarDataNode OfferService:  actual=" + actual
          + "; offerServiceAddress=" + offerServiceAddress);
      return actual.equalsIgnoreCase(offerServiceAddress);
    } catch (Exception ex) {
      LOG.error(offerServiceAddress + ": "
          + "AvatarDataNode OfferService: Could not get the primary from ZooKeeper", ex);
      // try {
      // zkClient.shutdown();
      // } catch (InterruptedException e) {
      // e.printStackTrace();
      // }
    }
    // try { //del by guoleitao
    // zkClient.shutdown();
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // } //end del
    return false;
  }

  public void run() {
    if (!shouldRun) {
      LOG.info(offerServiceAddress + ": shouldRun is " + shouldRun + ", OfferService quit.");
      return;
    }
    // del by guoleitao
    // KeepAliveHeartbeater keepAliveTask =
    // new KeepAliveHeartbeater(namenode, nsRegistration, this.servicePair);
    // keepAliveSender = Executors.newSingleThreadScheduledExecutor();
    // keepAliveRun = keepAliveSender.scheduleAtFixedRate(keepAliveTask, 0,
    // anode.heartBeatInterval,
    // TimeUnit.MILLISECONDS);
    while (shouldRun) {
      try {
        if (isPrimaryService()) {
          // servicePair.setPrimaryOfferService(this); //del
          this.anode.setPrimaryOfferService(this); // add by guoleitao
          LOG.info(offerServiceAddress
              + ": set this OfferService as primary service. isPrimaryOfferService =  "
              + this.anode.isPrimaryOfferService(this));
        }
        offerService();
      } catch (Exception e) {
        LOG.error(offerServiceAddress + ": "
            + "AvatarDataNode OfferService: OfferService encountered exception "
            + StringUtils.stringifyException(e));
      }
    }
    LOG.info(offerServiceAddress + ": shouldRun is " + shouldRun
        + ", Quit OfferService while loop.");
    stop();
  }

  private void setBackoff(boolean value) {
    synchronized (receivedAndDeletedBlockList) {
      this.shouldBackoff = value;
    }
  }

  public void offerService() throws Exception {

    LOG.info(offerServiceAddress + ": "
        + "AvatarDataNode OfferService: using BLOCKREPORT_INTERVAL of " + anode.blockReportInterval
        + "msec" + " Initial delay: " + anode.initialBlockReportDelay + "msec");
    // LOG.info("using DELETEREPORT_INTERVAL of " + anode.deletedReportInterval
    // + "msec");
    // LOG.info("using HEARTBEAT_EXPIRE_INTERVAL of " + anode.heartbeatExpireInterval + "msec");

    //
    // Now loop for a long time....
    //
    while (shouldRun) {
      try {

        // If we are falling behind in confirming blockReceived to NN, then
        // we clear the backlog and schedule a block report. This scenario
        // is likely to arise if one of the NN is down for an extended period.
        if (receivedAndDeletedBlockList.size() + receivedAndDeletedRetryBlockList.size() > backlogSize) {
          LOG.warn(offerServiceAddress + ": "
              + "The backlog of blocks to be confirmed has exceeded the "
              + " configured maximum of " + backlogSize
              + " records. Cleaning up and scheduling a block report.");
          synchronized (receivedAndDeletedBlockList) {
            receivedAndDeletedBlockList.clear();
            receivedAndDeletedRetryBlockList.clear();
          }
          scheduleBlockReport(0);
        }

        long startTime = AvatarDataNode.now();

        //
        // Every so often, send heartbeat or block-report
        //
        if (startTime - lastHeartbeat > anode.heartBeatInterval) {
          //
          // All heartbeat messages include following info:
          // -- Datanode name
          // -- data transfer port
          // -- Total capacity
          // -- Bytes remaining
          //
          setBackoff(false);
          lastHeartbeat = startTime;
          // DatanodeCommand[] cmds = avatarnode.sendHeartbeatNew(nsRegistration,
          // data.getCapacity(),
          // data.getDfsUsed(),
          // data.getRemaining(),
          // data.getNSUsed(
          // this.servicePair.namespaceId),
          // anode.xmitsInProgress.get(),
          // anode.getXceiverCount());
          // add by guoleitao
          DatanodeCommand[] cmds = avatarnode.sendHeartbeatNew(nsRegistration, data.getCapacity(),
            data.getDfsUsed(), data.getRemaining(), anode.xmitsInProgress.get(),
            anode.getXceiverCount(), data.getNumFailedVolumes());
          // end add

          // this.servicePair.lastBeingAlive = anode.now();//del
          // LOG.debug("Sent heartbeat at " + this.servicePair.lastBeingAlive);//del
          myMetrics.heartbeats.inc(AvatarDataNode.now() - startTime);
          // LOG.info("Just sent heartbeat, with name " + localName);
          if (!processCommand(cmds)) continue;
        }

        // check if there are newly received blocks (pendingReceivedRequeste > 0
        // or if the deletedReportInterval passed.

        // if (firstBlockReportSent && !shouldBackoff && (pendingReceivedRequests > 0
        // || (startTime - lastDeletedReport > anode.deletedReportInterval))) {//del by guoleitao
        if (pendingReceivedRequests > 0) LOG.debug(offerServiceAddress + ": "
            + "firstBlockReportSent=" + firstBlockReportSent + "; shouldBackoff" + shouldBackoff
            + "; pendingReceivedRequests=" + pendingReceivedRequests);
        if (firstBlockReportSent && !shouldBackoff && (pendingReceivedRequests > 0)) {// add by
                                                                                      // guoleitao
          LOG.info(offerServiceAddress + ": "
              + "firstBlockReportSent && !shouldBackoff && (pendingReceivedRequests > 0");
          // check if there are newly received blocks
          Block[] receivedAndDeletedBlockArray = null;
          int numBlocksReceivedAndDeleted = 0;
          int currentPendingRequests = 0;

          synchronized (receivedAndDeletedBlockList) {
            LOG.debug(offerServiceAddress + ": in synchronized (receivedAndDeletedBlockList)");
            // if the retry list is empty, send the receivedAndDeletedBlockList
            // if it's not empty, check the timer if we can retry
            if (receivedAndDeletedRetryBlockList.isEmpty()
                || (lastBlockReceivedFailed + blockReceivedRetryInterval < startTime)) {

              // process the retry list first
              // insert all blocks to the front of the block list
              // (we maintain the order in which entries were inserted)
              moveRetryAcks();

              // construct the ACKs array
              lastDeletedReport = startTime;
              numBlocksReceivedAndDeleted = receivedAndDeletedBlockList.size();
              if (numBlocksReceivedAndDeleted > 0) {
                receivedAndDeletedBlockArray = receivedAndDeletedBlockList
                    .toArray(new Block[numBlocksReceivedAndDeleted]);
                receivedAndDeletedBlockList.clear();
                currentPendingRequests = pendingReceivedRequests;
                pendingReceivedRequests = 0;
              }
            }
          }

          // process received + deleted
          // if exception is thrown, add all blocks to the retry list
          if (receivedAndDeletedBlockArray != null) {
            LOG.info(offerServiceAddress + ": "
                + " process received + deleted for receivedAndDeletedBlockArray = "
                + receivedAndDeletedBlockArray.toString());
            Block[] failed = null;
            try {
              failed = avatarnode.blockReceivedAndDeletedNew(nsRegistration,
                receivedAndDeletedBlockArray);
            } catch (Exception e) {
              synchronized (receivedAndDeletedBlockList) {
                for (int i = 0; i < receivedAndDeletedBlockArray.length; i++) {
                  receivedAndDeletedBlockList.add(0, receivedAndDeletedBlockArray[i]);
                }
                pendingReceivedRequests += currentPendingRequests;
                LOG.debug(offerServiceAddress + ": 2- Am I the primary OfferService? "
                    + this.anode.isPrimaryOfferService(this) + "! pendingReceivedRequests="
                    + pendingReceivedRequests);
              }
              throw e;
            }
            processFailedReceivedDeleted(failed, receivedAndDeletedBlockArray);
          }
        }

        // send block report
        if (startTime - lastBlockReport > anode.blockReportInterval) {
          if (shouldBackoff) {
            scheduleBlockReport(BACKOFF_DELAY);
            LOG.info(offerServiceAddress + ": " + "Backoff blockreport. Will be sent in "
                + (lastBlockReport + anode.blockReportInterval - startTime) + "ms");
          } else {
            //
            // Send latest blockinfo report if timer has expired.
            // Get back a list of local block(s) that are obsolete
            // and can be safely GC'ed.
            //
            long brStartTime = AvatarDataNode.now();
            // Block[] bReport = data.getBlockReport(servicePair.namespaceId); //del
            Block[] bReport = data.getBlockReport(); // add
            // Block[] bReport = data.retrieveAsyncBlockReport();//add by guoleitao
            DatanodeCommand cmd = avatarnode.blockReportNew(nsRegistration, new BlockReport(
                BlockListAsLongs.convertToArrayLongs(bReport)));
            if (cmd != null && cmd.getAction() == DatanodeProtocols.DNA_BACKOFF) {
              // The Standby is catching up and we need to reschedule
              scheduleBlockReport(BACKOFF_DELAY);
              continue;
            }

            firstBlockReportSent = true;
            long brTime = AvatarDataNode.now() - brStartTime;
            myMetrics.blockReports.inc(brTime);
            LOG.info(offerServiceAddress + ": " + "BlockReport of " + bReport.length
                + " blocks got processed in " + brTime + " msecs on " + namenodeAddress);
            if (resetBlockReportTime) {
              //
              // If we have sent the first block report, then wait a random
              // time before we start the periodic block reports.
              //
              lastBlockReport = startTime - R.nextInt((int) (anode.blockReportInterval));
              resetBlockReportTime = false;
            } else {

              /*
               * say the last block report was at 8:20:14. The current report should have started
               * around 9:20:14 (default 1 hour interval). If current time is : 1) normal like
               * 9:20:18, next report should be at 10:20:14 2) unexpected like 11:35:43, next report
               * should be at 12:20:14
               */
              lastBlockReport += (AvatarDataNode.now() - lastBlockReport)
                  / anode.blockReportInterval * anode.blockReportInterval;
            }
            processCommand(cmd);
          }
        }

        // start block scanner is moved to the Dataode.run()

        //
        // There is no work to do; sleep until hearbeat timer elapses,
        // or work arrives, and then iterate again.
        //
        long waitTime = anode.heartBeatInterval - (System.currentTimeMillis() - lastHeartbeat);
        // LOG.debug(offerServiceAddress+": "+
        // "heartBeatInterval = "+anode.heartBeatInterval+", waitTime = "+waitTime);
        synchronized (receivedAndDeletedBlockList) {
          if (waitTime > 0 && (shouldBackoff || pendingReceivedRequests == 0) && shouldRun) {
            // LOG.debug(offerServiceAddress+": "+
            // "waitTime > 0 && (shouldBackoff || pendingReceivedRequests == 0) && shouldRun");
            try {
              receivedAndDeletedBlockList.wait(waitTime);
            } catch (InterruptedException ie) {
            }
          }
        } // synchronized
      } catch (RemoteException re) {
        anode.handleRegistrationError(re);
      } catch (IOException e) {
        LOG.warn(e);
      }
    } // while (shouldRun)
    LOG.debug(offerServiceAddress + ": shouldRun=" + shouldRun + "; quit OfferService.");
  } // offerService

  public void moveRetryAcks() {
    // insert all blocks to the front of the block list
    // (we maintain the order in which entries were inserted)
    Iterator<Block> blkIter = receivedAndDeletedRetryBlockList.descendingIterator();
    while (blkIter.hasNext()) {
      Block blk = blkIter.next();
      receivedAndDeletedBlockList.add(0, blk);
      if (!DFSUtil.isDeleted(blk)) {
        pendingReceivedRequests++;
        LOG.debug(offerServiceAddress + ": 1- Am I the primary OfferService? "
            + this.anode.isPrimaryOfferService(this) + "! pendingReceivedRequests="
            + pendingReceivedRequests);
      }
    }
    receivedAndDeletedRetryBlockList.clear();
  }

  private void processFailedReceivedDeleted(Block[] failed, Block[] receivedAndDeletedBlockArray) {
    synchronized (receivedAndDeletedBlockList) {

      // Blocks that do not belong to an Inode are saved for
      // retransmisions
      for (int i = 0; i < failed.length; i++) {
        // Insert into retry list.
        LOG.info(offerServiceAddress + ": " + "Block " + failed[i]
            + " does not belong to any file " + "on namenode " + avatarnodeAddress
            + " Retry later.");
        receivedAndDeletedRetryBlockList.add(failed[i]);
        lastBlockReceivedFailed = AvatarDataNode.now();
      }
    }
  }

  /**
   * Process an array of datanode commands
   * @param cmds an array of datanode commands
   * @return true if further processing may be required or false otherwise.
   */
  private boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      // boolean isPrimary = this.servicePair.isPrimaryOfferService(this); //del by guoleitao
      boolean isPrimary = this.anode.isPrimaryOfferService(this); // add
      for (DatanodeCommand cmd : cmds) {
        try {
          LOG.debug(offerServiceAddress + ": Command = " + cmd.getAction());
          LOG.debug(offerServiceAddress + ": Who am I, primary ? " + isPrimary
              + "; but actually who am I from zk? " + isPrimaryService());
          if (cmd.getAction() != DatanodeProtocol.DNA_REGISTER
              && cmd.getAction() != DatanodeProtocols.DNA_BACKOFF && !isPrimary) {
            if (isPrimaryService()) {
              // The failover has occured. Need to update the datanode knowledge
              // this.servicePair.setPrimaryOfferService(this);//del
              LOG.info(offerServiceAddress + ": "
                  + "The failover has occured. Need to update the datanode knowledge.");
              this.anode.setPrimaryOfferService(this); // add
              synchronized (receivedAndDeletedBlockList) {
                // Need to move acks from the retry list to
                // blockReceivedAndDeleted list to
                // trigger immediate NN notification, since now
                // we are communicating with the primary.
                moveRetryAcks();
              }
            } else {
              continue;
            }
          } else if (cmd.getAction() == DatanodeProtocol.DNA_REGISTER && !isPrimaryService()) {
            // Standby issued a DNA_REGISTER. Enter the mode of sending frequent
            // heartbeats
            LOG.info(offerServiceAddress + ": "
                + "Registering with Standby. Start frequent block reports.");
            reportsSinceRegister = 0;
            if (isPrimary) {
              // This information is out of date
              LOG.info(offerServiceAddress + ": "
                  + "This information is out of date, set primary offer service as NULL.");
              // this.servicePair.setPrimaryOfferService(null); //del
              this.anode.setPrimaryOfferService(null); // add
            }
          }
          if (processCommand(cmd) == false) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn(offerServiceAddress + ": " + "Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }

  /**
   * @param cmd
   * @return true if further processing may be required or false otherwise.
   * @throws IOException
   */
  private boolean processCommand(DatanodeCommand cmd) throws IOException {
    if (cmd == null) return true;
    final BlockCommand bcmd = cmd instanceof BlockCommand ? (BlockCommand) cmd : null;

    switch (cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      LOG.debug(offerServiceAddress + ": AvatarDatanodeCommand action: DNA_TRANSFER");
      // Send a copy of a block to another datanode
      // anode.transferBlocks(servicePair.namespaceId, bcmd.getBlocks(), bcmd.getTargets()); //del
      anode.transferBlocks(bcmd.getBlocks(), bcmd.getTargets()); // add
      myMetrics.blocksReplicated.inc(bcmd.getBlocks().length);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      LOG.debug(offerServiceAddress + ": AvatarDatanodeCommand action: DNA_INVALIDATE");
      //
      // Some local block(s) are obsolete and can be
      // safely garbage-collected.
      //
      Block toDelete[] = bcmd.getBlocks();
      try {
        if (anode.blockScanner != null) {
          // TODO temporary
          // anode.blockScanner.deleteBlocks(servicePair.namespaceId, toDelete);//del
          anode.blockScanner.deleteBlocks(toDelete);// add
        }
        // servicePair.removeReceivedBlocks(toDelete);//del
        this.anode.removeReceivedBlocks(toDelete);// add
        // data.invalidate(servicePair.namespaceId, toDelete);//del
        data.invalidate(toDelete);// add
      } catch (IOException e) {
        anode.checkDiskError();
        throw e;
      }
      myMetrics.blocksRemoved.inc(toDelete.length);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // shut down the data node
      // servicePair.shutdown();//del
      LOG.debug(offerServiceAddress + ": AvatarDatanodeCommand action: DNA_SHUTDOWN");
      this.anode.shutdown();
      return false;
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration - at start or if NN lost contact
      LOG.info(offerServiceAddress + ": " + "AvatarDatanodeCommand action: DNA_REGISTER");
      if (shouldRun) {
        // servicePair.register(namenode, namenodeAddress); //del
        this.anode.register(namenode, namenodeAddress); // add
        firstBlockReportSent = false;
        scheduleBlockReport(0);
      }
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      LOG.info(offerServiceAddress + ": " + "AvatarDatanodeCommand action: DNA_FINALIZE");
      anode.getStorage().finalizeUpgrade();
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      LOG.info(offerServiceAddress + ": " + "AvatarDatanodeCommand action: UC_ACTION_START_UPGRADE");
      // start distributed upgrade here
      // servicePair.processUpgradeCommand((UpgradeCommand)cmd);//del
      this.anode.upgradeManager.processUpgradeCommand((UpgradeCommand) cmd); // add
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      // anode.recoverBlocks(servicePair.namespaceId, bcmd.getBlocks(), bcmd.getTargets());//del
      LOG.info(offerServiceAddress + ": AvatarDatanodeCommand action: DNA_RECOVERBLOCK");
      anode.recoverBlocks(bcmd.getBlocks(), bcmd.getTargets());// add
      break;
    case DatanodeProtocols.DNA_BACKOFF:
      LOG.info(offerServiceAddress + ": "
          + "AvatarDatanodeCommand action: DNA_BACKOFF; shouldBackoff = " + shouldBackoff);
      setBackoff(true);
      break;
    default:
      LOG.warn(offerServiceAddress + ": " + "Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }

  /**
   * This methods arranges for the data node to send the block report at the next heartbeat.
   */
  public void scheduleBlockReport(long delay) {
    if (delay > 0) { // send BR after random delay
      lastBlockReport = System.currentTimeMillis()
          - (anode.blockReportInterval - R.nextInt((int) (delay)));
      LOG.info(offerServiceAddress + ": BlockReport delay is " + delay + "ms, now lastBlockReport="
          + lastBlockReport);
    } else { // send at next heartbeat
      lastBlockReport = lastHeartbeat - anode.blockReportInterval;
    }
    resetBlockReportTime = true; // reset future BRs for randomness
  }

  /**
   * Only used for testing
   */
  public void scheduleBlockReceivedAndDeleted(long delay) {
    // if (delay > 0) {
    // lastDeletedReport = System.currentTimeMillis()
    // - anode.deletedReportInterval + delay;
    // } else {
    // lastDeletedReport = 0;
    // }//del by guoleitao
    lastDeletedReport = 0; // add
  }

  /**
   * Add a block to the pending received/deleted ACKs. to inform the namenode that we have received
   * a block.
   */
  void notifyNamenodeReceivedBlock(Block block, String delHint) {
    if (block == null) {
      throw new IllegalArgumentException("Block is null");
    }
    if (delHint != null && !delHint.isEmpty()) {
      block = new ReceivedBlockInfo(block, delHint);
    }
    // if (this.servicePair.isPrimaryOfferService(this)) {
    if (this.anode.isPrimaryOfferService(this)) { // add by guoleitao
      // primary: notify NN immediately
      synchronized (receivedAndDeletedBlockList) {
        receivedAndDeletedBlockList.add(block);
        pendingReceivedRequests++;
        LOG.info(offerServiceAddress + ":ReceivedBlock -I am primary? "
            + this.anode.isPrimaryOfferService(this) + "! pendingReceivedRequests="
            + pendingReceivedRequests);
        if (!shouldBackoff) {
          receivedAndDeletedBlockList.notifyAll();
        }
      }
    } else { // standby: delay notification by adding to retry list
      synchronized (receivedAndDeletedBlockList) {
        receivedAndDeletedRetryBlockList.add(block);
        LOG.info(offerServiceAddress + ":ReceivedBlock -I am standby? "
            + !this.anode.isPrimaryOfferService(this) + "! pendingReceivedRequests="
            + pendingReceivedRequests);
      }
    }
  }

  /**
   * Add a block to the pending received/deleted ACKs. to inform the namenode that we have deleted a
   * block.
   */
  void notifyNamenodeDeletedBlock(Block block) {
    if (block == null) {
      throw new IllegalArgumentException("Block is null");
    }
    // mark it as a deleted block
    DFSUtil.markAsDeleted(block);
    // if (this.servicePair.isPrimaryOfferService(this)) { //del
    if (this.anode.isPrimaryOfferService(this)) { // add
      // primary: notify NN immediately
      synchronized (receivedAndDeletedBlockList) {
        receivedAndDeletedBlockList.add(block);
      }
      LOG.info(offerServiceAddress + ": DeletedBlock -I am primary? "
          + this.anode.isPrimaryOfferService(this) + "! receivedAndDeletedBlockList="
          + receivedAndDeletedBlockList.size());
    } else { // standby: delay notification by adding to the retry list
      synchronized (receivedAndDeletedBlockList) {
        receivedAndDeletedRetryBlockList.add(block);
      }
      LOG.info(offerServiceAddress + ":DeletedBlock -I am standby? "
          + !this.anode.isPrimaryOfferService(this) + "! receivedAndDeletedBlockList="
          + receivedAndDeletedBlockList.size());
    }
  }

  /**
   * Remove blocks from blockReceived queues
   */
  void removeReceivedBlocks(Block[] removeList) {
    synchronized (receivedAndDeletedBlockList) {
      ReceivedBlockInfo block = new ReceivedBlockInfo();
      block.setDelHints(ReceivedBlockInfo.WILDCARD_HINT);
      for (Block bi : removeList) {
        block.set(bi.getBlockId(), bi.getNumBytes(), bi.getGenerationStamp());
        while (receivedAndDeletedBlockList.remove(block)) {
          LOG.info(offerServiceAddress + ": "
              + "Block deletion command deleted from receivedDeletedBlockList " + bi);
        }
        while (receivedAndDeletedRetryBlockList.remove(block)) {
          LOG.info(offerServiceAddress + ": "
              + "Block deletion command deleted from receivedDeletedRetryBlockList " + bi);
        }
      }
    }
  }

  void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    try {
      namenode.reportBadBlocks(blocks);
    } catch (IOException e) {
      /*
       * One common reason is that NameNode could be in safe mode. Should we keep on retrying in
       * that case?
       */
      LOG.warn(offerServiceAddress + ": " + "Failed to report bad block to namenode : "
          + " Exception : " + StringUtils.stringifyException(e));
      throw e;
    }
  }

  // this fun del by guoleitao, 20121009
  /** Block synchronization */
  // LocatedBlock syncBlock(
  // Block block, List<BlockRecord> syncList,
  // boolean closeFile, List<InterDatanodeProtocol> datanodeProxies
  // )//del by guoleitao
  LocatedBlock syncBlock(Block block, List<BlockRecord> syncList, DatanodeInfo[] targets,
      boolean closeFile) // add by guoleitao
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(offerServiceAddress + ": " + "block=" + block + ", (length=" + block.getNumBytes()
          + "), syncList=" + syncList + ", closeFile=" + closeFile);
    }

    // syncList.isEmpty() that all datanodes do not have the block
    // so the block can be deleted.
    if (syncList.isEmpty()) {
      namenode.commitBlockSynchronization(block, 0, 0, closeFile, true, DatanodeID.EMPTY_ARRAY);
      // return null;//del by guoleitao
      LocatedBlock b = new LocatedBlock(block, targets);
      // if (isBlockTokenEnabled) {
      // b.setBlockToken(blockTokenSecretManager.generateToken(null, b.getBlock(),
      // EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE)));
      // }
      return b;// add by guoleitao
    }

    List<DatanodeID> successList = new ArrayList<DatanodeID>();

    long generationstamp = namenode.nextGenerationStamp(block, closeFile);
    Block newblock = new Block(block.getBlockId(), block.getNumBytes(), generationstamp);

    for (BlockRecord r : syncList) {
      try {
        // r.datanode.updateBlock(servicePair.namespaceId, r.info.getBlock(), newblock,
        // closeFile);//del
        r.datanode.updateBlock(r.info.getBlock(), newblock, closeFile);// add
        successList.add(r.id);
      } catch (IOException e) {
        InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock=" + newblock
            + ", datanode=" + r.id + ")", e);
      }
    }

    // anode.stopAllProxies(datanodeProxies); //del by guoleitao

    if (!successList.isEmpty()) {
      DatanodeID[] nlist = successList.toArray(new DatanodeID[successList.size()]);

      namenode.commitBlockSynchronization(block, newblock.getGenerationStamp(),
        newblock.getNumBytes(), closeFile, false, nlist);
      DatanodeInfo[] info = new DatanodeInfo[nlist.length];
      for (int i = 0; i < nlist.length; i++) {
        info[i] = new DatanodeInfo(nlist[i]);
      }
      return new LocatedBlock(newblock, info); // success
    }

    // failed
    StringBuilder b = new StringBuilder();
    for (BlockRecord r : syncList) {
      b.append("\n  " + r.id);
    }
    throw new IOException(offerServiceAddress + ": " + "Cannot recover " + block
        + ", none of these " + syncList.size() + " datanodes success {" + b + "\n}");
  }
}
