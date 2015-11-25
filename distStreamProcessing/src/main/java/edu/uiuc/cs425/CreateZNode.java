package edu.uiuc.cs425;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class CreateZNode {

	private static ZooKeeper 						m_oZooKeeper;
	private static ZooKeeperConnector				m_oZooKeeperConnector;
	
	private static void create(String path, byte[] data)
	{
		try {
			//Persistent znode - stay in ensemble until client closes it
			//Ephemeral znode  - temporary
			m_oZooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private static void main(String [] args)
	{
		String path = "/sampleZNode";
		byte [] data = "sample data".getBytes();
		
		m_oZooKeeperConnector = new ZooKeeperConnector();
		try {
			m_oZooKeeperConnector.connect("localhost");
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		create(path, data);
	}
	
	
}
