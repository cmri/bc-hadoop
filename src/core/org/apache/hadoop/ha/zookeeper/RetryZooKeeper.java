/**
 * 
 */
package org.apache.hadoop.ha.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * @author ltguo
 * this class is defined for hadoop high availability
 */
public class RetryZooKeeper {
  private static final Log LOG = LogFactory.getLog(RetryZooKeeper.class);
  // the actual ZooKeeper client instance
  private ZooKeeper zk;
  private final RetryCounterFactory retryCounterFactory;
  
  public RetryZooKeeper(String quorumServers, int seesionTimeout,
	      Watcher watcher, int maxRetries, int retryIntervalMillis) 
	      throws IOException {
	  this.zk = new ZooKeeper(quorumServers, seesionTimeout, watcher);
	  this.retryCounterFactory =
			  new RetryCounterFactory(maxRetries, retryIntervalMillis);
  }
	
  public String create(String path, byte[] data, List<ACL> acl, 
	      CreateMode createMode) throws KeeperException, InterruptedException {
	    RetryCounter retryCounter = retryCounterFactory.create();
	    boolean isRetry = false; // False for first attempt, true for all retries.
	    while (true) {
	      try {
	        return zk.create(path, data, acl, createMode);
	      } catch (KeeperException e) {
	        switch (e.code()) {
	          case NODEEXISTS:
	            if (isRetry) {
	              // If the connection was lost, there is still a possibility that
	              // we have successfully created the node at our previous attempt,
	              // so we read the node and compare. 
	              byte[] currentData = zk.getData(path, false, null);
	              if (currentData != null &&
	                  currentData.toString().compareToIgnoreCase(data.toString()) == 0) { 
	                // We successfully created a non-sequential node
	                return path;
	              }
	              LOG.error("Node " + path + " already exists with " + 
	                  currentData.toString() + ", could not write " +
	                  data.toString());
	              throw e;
	            }
	            LOG.error("Node " + path + " already exists and this is not a " +
	                "retry");
	            throw e;

	          case CONNECTIONLOSS:
	          case OPERATIONTIMEOUT:
	            LOG.warn("Possibly transient ZooKeeper exception: " + e);
	            if (!retryCounter.shouldRetry()) {
	              LOG.error("ZooKeeper create failed after "
	                + retryCounter.getMaxRetries() + " retries");
	              throw e;
	            }
	            break;

	          default:
	            throw e;
	        }
	      }
	      retryCounter.sleepUntilNextRetry();
	      retryCounter.useRetry();
	      isRetry = true;
	    }
	  }
  
  
  
  /**
   * exists is an idempotent operation. Retry before throw out exception
   * @param path
   * @param watcher
   * @return A Stat instance
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Stat exists(String path, Watcher watcher)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.exists(path, watcher);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper exists failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  /**
   * exists is an idempotent operation. Retry before throw out exception
   * @param path
   * @param watch
   * @return A Stat instance
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Stat exists(String path, boolean watch)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.exists(path, watch);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper exists failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }
	
  /**
   * getData is an idempotent operation. Retry before throw out exception
   * @param path
   * @param watcher
   * @param stat
   * @return Data
   * @throws KeeperException
   * @throws InterruptedException
   */
  public byte[] getData(String path, Watcher watcher, Stat stat)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        byte[] revData = zk.getData(path, watcher, stat);       
        return revData;
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getData failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  /**
   * getData is an idemnpotent operation. Retry before throw out exception
   * @param path
   * @param watch
   * @param stat
   * @return Data
   * @throws KeeperException
   * @throws InterruptedException
   */
  public byte[] getData(String path, boolean watch, Stat stat)
  throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        byte[] revData = zk.getData(path, watch, stat);
        return revData;
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getData failed after "
                + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }
  
  public void close() throws InterruptedException {
	    zk.close();
	  }

  public States getState() {
	    return zk.getState();
	  }

	  public ZooKeeper getZooKeeper() {
	    return zk;
	  }
}
