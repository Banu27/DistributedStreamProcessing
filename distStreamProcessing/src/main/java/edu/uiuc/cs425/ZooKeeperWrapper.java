package edu.uiuc.cs425;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZooKeeperWrapper {

	private ZooKeeper				m_oZooKeeper;
	private ZooKeeperConnector		m_oZooKeeperConnector;
	private Logger					m_oLogger;
	
	public ZooKeeper createZKInstance(String host, Watcher watcherclass ) throws IOException
	{
		return new ZooKeeper(host, 5000, watcherclass) ;
	}
	
	public void Initialize(String ConnectionIP, Logger oLogger)
	{
		m_oZooKeeperConnector = new ZooKeeperConnector();
		m_oLogger = oLogger;
		m_oLogger.Info("Initializing ZK");
		try {
			m_oZooKeeper = m_oZooKeeperConnector.connect(ConnectionIP);
			m_oLogger.Info("Connected to ZK");
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
	}
	
	public List<String> GetChildren(String zNodePath)
	{
		try {
			return m_oZooKeeper.getChildren(zNodePath, false);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
	public String read(String pathToZnode) throws KeeperException, InterruptedException, UnsupportedEncodingException
	{
	
		byte [] data;
		
		data = m_oZooKeeper.getData(pathToZnode, true, m_oZooKeeper.exists(pathToZnode, true));
		
		//for(byte b: data)
		//{
		//	System.out.println(b);
		//}
		
		String dataString = new String(data,"UTF-8");
		
		return dataString;
	}
	
	public void create(String path, String data, StringCallback createNodeCallback) throws KeeperException, InterruptedException, IllegalStateException, IOException
	{
		byte [] dataByte = data.getBytes();
		//Persistent znode - stay in ensemble until client closes it
		//Ephemeral znode  - temporary
		m_oZooKeeper.create(path, dataByte, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,createNodeCallback, null);
		
	}

	public void getChildren(String zNodePath, Watcher workersChangeWatcher, ChildrenCallback workersGetChildrenCallback,
			Object object) {
		// TODO Auto-generated method stub
		m_oZooKeeper.getChildren(zNodePath, workersChangeWatcher, workersGetChildrenCallback, object);
		
	}

	public void getChildren(String zNodePath, boolean b, ChildrenCallback workerAssignmentCallback, Object object) {
		// TODO Auto-generated method stub
		m_oZooKeeper.getChildren(zNodePath, b, workerAssignmentCallback, object);
	}

	public void getData(String zNodePath, Watcher dataChangeWatcher, DataCallback componentDataCallback, Object object) {
		// TODO Auto-generated method stub
		m_oZooKeeper.getData(zNodePath, dataChangeWatcher, componentDataCallback, object);
	}
	
	public void setData(String zNodePath, String value)
	{
		try {
			m_oZooKeeper.setData(zNodePath, value.getBytes(), -1);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void deleteZNode(String zNodePath) throws InterruptedException, KeeperException
	{
		m_oZooKeeper.delete(zNodePath,-1);
	}
	
	//public exists(String zNodePath) throws KeeperException, InterruptedException
	//{
	//	m_oZooKeeper.exists(zNodePath, true);
	//}
	
}
	
