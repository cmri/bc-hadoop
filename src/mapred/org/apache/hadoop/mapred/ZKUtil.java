package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZKUtil {
	
	private ZKUtil(){}
	public static final Log LOG = LogFactory.getLog(ZKUtil.class);
	static{
	    Configuration.addDefaultResource("mapred-default.xml");
	    Configuration.addDefaultResource("mapred-site.xml");
	  }
	//add by dengpeng 20120801 :for JT_HA
	 private static String zkquorumHost;
	 private static int sessiontimeout;
	 /***
	   * add configuration to mapred-site.xml
	   * 1,mapred.zookeeper.quorum
	   * 2,mapred.zookeeper.property.clientPort
	   * 3,zookeeper.session.timeout
	   */   
	 static{
		 JobConf zkconf=new JobConf();
		 String quorumHost=zkconf.get("mapred.zookeeper.quorum", "localhost");
		    LOG.info("JTHA: quorumHost is:"+quorumHost);
		    String quorumPort=zkconf.get("mapred.zookeeper.property.clientPort", "2181");
		    LOG.info("JTHA: quorumPort is:" +quorumPort);
		    sessiontimeout=zkconf.getInt("zookeeper.session.timeout", 3000);
		    LOG.info("JTHA: the sessiontimeout is:"+sessiontimeout);
		    StringTokenizer st=new StringTokenizer(quorumHost,",");
		    StringBuilder sb=new StringBuilder();
		    while(st.hasMoreTokens()){
		    	sb.append(st.nextToken()+":"+quorumPort+",");
		    }
		    zkquorumHost=sb.toString();
		    LOG.info("JTHA: the zkquorumHost is:"+zkquorumHost);
	 }	 
	 public static ZooKeeper getZK(){		 	 
		 boolean connectStatus=false;
		 ZooKeeper zk=null;   
		 int retrytime=3;
		    while(retrytime>0&&!connectStatus){
		    try{
		    	 zk=new ZooKeeper(zkquorumHost, sessiontimeout, new Watcher(){
					@Override
					public void process(WatchedEvent event) {
						// TODO Auto-generated method stub
						LOG.info("JTHA: zk happened a event:"+event.toString());						
					}    		 
		    	 });
		    connectStatus=true;
		    LOG.info("JTHA: the success retry time is:"+(4-retrytime));
		    }catch(IOException ioe){
		        LOG.debug("JTHA: network failure");
		        connectStatus=false;
		        retrytime--;
		    }
		    }
		    if(!connectStatus)
		    {
				try {
					throw new IOException("after retry "+retrytime+" time,failure connecte zookeeper"+zkquorumHost);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				}	
		    return zk;
	 }
}