package edu.uiuc.cs425;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class TestZooKeeperConnection {

	private static ZooKeeper 				m_oZooKeeper;
	private static ZooKeeperConnector		m_oZooKeeperConnector;
	private static List<String>				m_lZNodeList;
	
	void Initialize()
	{
		m_lZNodeList = new ArrayList<String>();
	}
	
	public static void main(String [] args)
	{
		m_oZooKeeperConnector = new ZooKeeperConnector();
		
		try {
			m_oZooKeeper = m_oZooKeeperConnector.connect("localhost");
			
			m_lZNodeList = m_oZooKeeper.getChildren("/",true);
			
			for(String zNode : m_lZNodeList)
			{
				System.out.println(zNode);
			}
			
			
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} //Check local host 
		catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
