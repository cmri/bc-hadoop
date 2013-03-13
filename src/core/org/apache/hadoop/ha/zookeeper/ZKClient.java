package org.apache.hadoop.ha.zookeeper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZKClient {

	private static final Log LOG = LogFactory.getLog(ZKClient.class);
	private static final String NAMENODE_HA_ZK_QUORUM = "fs.ha.zookeeper.quorum";
	
	// Making it large enough to be sure that the cluster is down
	// these retries go one after another so they do not take long
	public static final int ZK_CONNECT_TIMEOUT_DEFAULT = 10000; // 10 seconds

	private Configuration conf;
	private String quorumServers;
	private RetryZooKeeper zk;
	private Watcher watcher;

	public ZKClient(Configuration conf) throws IOException {
		this(conf, null);
	}

	public ZKClient(Configuration conf, Watcher watcher) throws IOException {
		this.conf = conf;
		this.watcher = watcher;
		this.quorumServers = conf.get(NAMENODE_HA_ZK_QUORUM);
	}

	/**
	 * initialize RetryZooKeeper if zk is null.
	 * */
	private void initZK() throws IOException {
		if (this.zk == null) {
			if (quorumServers == null) {
				throw new IOException(
						"Unable to determine ZooKeeper quorumservers");
			}
			int timeout = conf.getInt("zookeeper.session.timeout", 180 * 1000);
			LOG.debug(" opening connection to ZooKeeper with quorumservers ("
					+ quorumServers + ")");
			int retry = conf.getInt("zookeeper.recovery.retry", 10);
			int retryIntervalMillis = conf.getInt(
					"zookeeper.recovery.retry.intervalmill", 1000);

			this.zk = new RetryZooKeeper(quorumServers, timeout, watcher,
					retry, retryIntervalMillis);
		}
		if (zk.getState() != ZooKeeper.States.CONNECTED) {
			try {
				Thread.sleep(ZK_CONNECT_TIMEOUT_DEFAULT); //10s
			} catch (InterruptedException e) {
			}
		}
		if (zk.getState() != ZooKeeper.States.CONNECTED) {
			throw new IOException("Timed out trying to connect to ZooKeeper");
		}
	}

	/**
	 * Watch the specified znode for delete/create/change events. The watcher is
	 * set whether or not the node exists. If the node already exists, the
	 * method returns true. If the node does not exist, the method returns
	 * false.
	 * 
	 * @param znode
	 *            path of node to watch
	 * @return true if znode exists, false if does not exist or error
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 */
	public boolean watchAndCheckExists(String znode) {
		if ((znode == null) ||(znode.length()==0)) {
			LOG.info("znode is empty");
			return false;
		}
		try {
			initZK();
		} catch (IOException e1) {
			e1.printStackTrace();
			return false;
		}
		try {
			Stat s = zk.exists(znode, watcher);
			LOG.info("Set watcher on  znode " + znode);
			return s != null ? true : false;
		} catch (KeeperException e) {
			LOG.warn("Unable to set watcher on znode " + znode, e);
			return false;
		} catch (InterruptedException e) {
			LOG.warn("Unable to set watcher on znode " + znode, e);
			return false;
		}
	}
	
	/**
	 * Set the specified znode to be an ephemeral node carrying the specified
	 * data.
	 * 
	 * If the node is created successfully, a watcher is also set on the node.
	 * 
	 * If the node is not created successfully because it already exists, this
	 * method will also set a watcher on the node.
	 * 
	 * If there is another problem, a KeeperException will be thrown.
	 * 
	 * @param znode
	 *            path of node
	 * @param data
	 *            data of node
	 * @return true if node created, false if not, watch set in both cases
	 * @throws KeeperException
	 *             if unexpected zookeeper exception
	 **/

	public boolean createEphemeralNodeAndWatch(String znode, byte[] data) {
		if ((znode == null) ||(znode.length()==0)||(data == null) ||(data.length==0)) {
			LOG.info("znode or data is empty");
			return false;
		}
		try {
			initZK();
			zk.create(znode, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (IOException e2) {
			e2.printStackTrace();
			return false;
		} catch (KeeperException.NodeExistsException nee) {
			if (!watchAndCheckExists(znode)) {
				return createEphemeralNodeAndWatch(znode, data);
			}
			return false;
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Get znode data. Does not set a watcher.
	 * 
	 * @return ZNode data
	 */
	public byte[] getDataNoWatch(String znode) throws KeeperException {
		if ((znode == null) || (znode.length() == 0)) {
			LOG.info("znode is empty");
			return null;
		}
		try {
			initZK();
			byte[] data = zk.getData(znode, false, null);
			return data;
		} catch (KeeperException.NoNodeException e) {
			LOG.debug("Unable to get data of znode " + znode + " "
					+ "because node does not exist (not an error)");
			return null;
		} catch (KeeperException e) {
			LOG.warn("Unable to get data of znode " + znode, e);
			return null;
		} catch (InterruptedException e) {
			LOG.warn("Unable to get data of znode " + znode, e);
			Thread.currentThread().interrupt();
			return null;
		} catch (IOException e) {
			LOG.warn("initZK error.");
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Get znode data. Does not set a watcher.
	 * 
	 * @return ZNode data
	 */
	public byte[] getDataAndWatch(String znode, Stat stat)
			throws KeeperException {
		if ((znode == null) || (znode.length() == 0)) {
			LOG.info("znode is empty");
			return null;
		}
		if (this.watcher == null) {
			LOG.info("Unable to watch on znode "
					+ znode
					+ ", because there is no explict watcher set for you zkClient.");
			return null;
		}
		try {
			initZK();
			byte[] data = zk.getData(znode, this.watcher, stat);
			return data;
		} catch (KeeperException.NoNodeException e) {
			LOG.debug("Unable to get data of znode " + znode + " "
					+ "because node does not exist (not an error)");
			return null;
		} catch (KeeperException e) {
			LOG.warn("Unable to get data of znode " + znode, e);
			return null;
		} catch (InterruptedException e) {
			LOG.warn("Unable to get data of znode " + znode, e);
			Thread.currentThread().interrupt();
			return null;
		} catch (IOException e) {
			LOG.warn("initZK error.");
			e.printStackTrace();
			return null;
		}
	}
	
	
	public void stopZK() throws InterruptedException {
		if (zk == null)
			return;
		zk.close();
		zk = null;
	}

}
