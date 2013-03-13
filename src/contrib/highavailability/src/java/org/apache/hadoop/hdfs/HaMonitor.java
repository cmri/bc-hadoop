package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AvatarZooKeeperClient;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class HaMonitor implements Runnable, Watcher  {
  public static final Log LOG = LogFactory.getLog(AvatarNode.class.getName());
	AvatarNode avatar;
	private AvatarZooKeeperClient zkClient;
	private Configuration conf;
//	private String prefix;
  private final String activeZnode = AvatarZooKeeperClient.activeZnode;
  private final String standbyZnode=AvatarZooKeeperClient.standbyZnode;
	String hostname = "empty";
	// check whether there is active avatarNN in the cluster
	final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);
  private boolean firstConnZK = true; // Does this zk client is the first time to connect to zk? 
                                      // If not, the connection maybe has disconnect before. 

	public HaMonitor(AvatarNode avatarNode, Configuration conf) {
		this.avatar = avatarNode;
		this.conf = conf;
	}

	@Override
  public void run() {
	  LOG.info("HaMonitor: thread: "+Thread.currentThread().getName()+" started.");
    try {
      // hostname should be 'hostname:9000' pair
      hostname = InetAddress.getLocalHost().getHostName() + ":"
          + NameNode.getServiceAddress(conf, true).getPort();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
//    prefix = conf.get("fs.ha.zookeeper.prefix", "/hdfs");
    zkClient = new AvatarZooKeeperClient(conf, this);
    if (blockUntilBecomingActiveMaster(zkClient)) {
      LOG.info("HaMonitor: "+hostname + " is the active avatarNN now");
    }
  }

	public void stop() {
		try {
			this.zkClient.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    LOG.info("HaMonitor: thread: "+Thread.currentThread().getName()+" stoped.");
	}

	@Override
  public void process(WatchedEvent event) {
    LOG.info("HaMonitor: " + hostname + " received ZooKeeper Event, " + "type=" + event.getType()
        + ", " + "state=" + event.getState() + ", " + "path=" + event.getPath());

    switch (event.getType()) {
    case None: {
      switch (event.getState()) {
      case Disconnected: {
        // The client is in the disconnected state - it is not connected to any server in the
        // ensemble
        LOG.warn("HaMonitor: " + hostname + "has lost connectione to Zookeeper Server "
            + zkClient.getConnection() + ". Stop avatar now.");
        stopAvatarByHaMonitor();
        break;
      }
      case Expired: {
        // The serving cluster has expired this session. The ZooKeeper client connection (the
        // session) is no longer valid. You must create a new client connection (instantiate a new
        // ZooKeeper instance) if you with to access the ensemble.
        LOG.warn("HaMonitor: " + hostname + "'s connectione to Zookeeper Server "
            + zkClient.getConnection() + " has Expired! Stop avatar now.");
        stopAvatarByHaMonitor();
        break;
      }
      case SyncConnected: {
        // The client has connected to zookeeper server now. But is it the 1st or 2nd time to get
        // SyncConnected event? If this is the first time, it's ok. If this is not the first time to
        // receive SyncConnected event, there must be sth wrong with the network before. then we
        // will shutdown this avatarnode.
        if (firstConnZK) { // this is the first time to connect to zk server
          LOG.debug("HaMonitor: " + hostname + " is 1st time to receive SyncConnected event.");
          firstConnZK = false;
        } else {
          LOG.warn("HaMonitor: "
              + hostname
              + " is NOT 1st time to receive SyncConnected event. Something wrong with the connection before!");
          stopAvatarByHaMonitor();
        }
        break;
      }
      }
    }
    case NodeDeleted: {
      if (null != event.getPath()) {
        nodeDeleted(event.getPath());
      }
      break;
    }
    }
  }
	
	private void stopAvatarByHaMonitor(){
    try {      
      avatar.shutdownAvatar();
    } catch (IOException e) {
      e.printStackTrace();
    }
	}

	public void nodeDeleted(String path) {
	  LOG.info("HaMonitor: There is znode deleted, znode = "+path);
		if (path.equals(activeZnode)) {
		  LOG.warn("HaMonitor: Active Namenode down, because znode ("+activeZnode+") is deleted.");
			handleMasterNodeChange();
		}
	}

	private void handleMasterNodeChange() {
		// Watch the node and check if it exists.
		if (zkClient.watchAndCheckExists(activeZnode)) {
			LOG.info("HaMonitor: A master is now available");
			clusterHasActiveMaster.set(true);
		} else {
			LOG.info("HaMonitor: No master available. Notifying waiting threads");
			synchronized (clusterHasActiveMaster) {
				clusterHasActiveMaster.set(false);
				// Notify any thread waiting to become the active master
				clusterHasActiveMaster.notifyAll();
			}
		}
	}

	boolean blockUntilBecomingActiveMaster(AvatarZooKeeperClient zkc) {
		if (zkc.createEphemeralNodeAndWatch(activeZnode, this.hostname.getBytes())) {		  
      try {
        String currentPrimary = zkc.getPrimaryAvatarAddress(hostname, new Stat(), false);
        LOG.info("HaMonitor: the current Primary AvatarNode is " + currentPrimary);
        if ((avatar.getAvatar() == Avatar.STANDBY) && (currentPrimary.equalsIgnoreCase(hostname))) {
          LOG.info("HaMonitor: this avatar (" + currentPrimary + ") is now primary, which is standby before.");
          // add by guoleitao, 20121214
          if (zkc.deleteNode(standbyZnode)) { // del standby from standbyZnode on zk
            this.clusterHasActiveMaster.set(true);
            // end add
            LOG.info("HaMonitor: Successfully delete this avatar (" + currentPrimary + ") from standbyZnode ("
                + standbyZnode + ").");
            avatar.setAvatar(Avatar.ACTIVE);
            LOG.info("HaMonitor: Finish set this avatar as ACTIVE. Now this avatar is "
                + avatar.getAvatar().toString());
            return true;
          } else {
            LOG.warn("HaMonitor: fail to delete myself from standbyZnode, exit this avatar ...");
            stopAvatarByHaMonitor();
          }
        } else {
          if (currentPrimary.equalsIgnoreCase(hostname)) {
            this.clusterHasActiveMaster.set(true);
            LOG.info("HaMonitor: Error, can not set " + hostname
                + " from ACTIVE to ACTIVE. Maybe this node has come back again as the Primary.");
          } else {
            LOG.warn("HaMonitor: The current avatar node (" + currentPrimary + ") is not equal to hostname ("
                + hostname + "). exit this avatar...");
            stopAvatarByHaMonitor();
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (KeeperException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } else {
			try {
				synchronized (this.clusterHasActiveMaster) {
					clusterHasActiveMaster.wait();
				}
			} catch (InterruptedException e) {
				LOG.info("HaMonitor: Interrupted waiting for active avatarNN to die", e);
				e.printStackTrace();
			}
		}
		blockUntilBecomingActiveMaster(zkc);
		return false;
	}
}
