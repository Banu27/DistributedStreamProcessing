package edu.uiuc.cs425;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

 
public class ZooKeeperConnector{

	
	private ZooKeeper 							m_oZooKeeper;
	private java.util.concurrent.CountDownLatch m_oConnectedSignal;
	
	public ZooKeeperConnector() {
		// TODO Auto-generated constructor stub
		m_oConnectedSignal = new java.util.concurrent.CountDownLatch(1);
	}
	
	public ZooKeeper connect(String host) throws IOException, InterruptedException, IllegalStateException
	{
		//host - local host
		//5000 - number of milliseconds to try to connect to zookeeper
		m_oZooKeeper = new ZooKeeper(host, 5000, new Watcher() { 
			public void process(WatchedEvent event) {
				if(event.getState() == KeeperState.SyncConnected)
				{
					m_oConnectedSignal.countDown();
				}
			}
		});
		
		m_oConnectedSignal.await();
		return m_oZooKeeper;
	}
	
}
