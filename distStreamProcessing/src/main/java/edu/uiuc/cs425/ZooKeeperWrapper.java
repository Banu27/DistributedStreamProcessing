package edu.uiuc.cs425;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZooKeeperWrapper {

	private static ZooKeeper				m_oZooKeeper;
	private static ZooKeeperConnector		m_oZooKeeperConnector;
	
	public ZooKeeper createZKInstance(String host, Watcher watcherclass ) throws IOException
	{
		return new ZooKeeper(host, 5000, watcherclass) ;
	}
	
	public void Initialize(String ConnectionIP)
	{
		m_oZooKeeperConnector = new ZooKeeperConnector();
		try {
			m_oZooKeeper = m_oZooKeeperConnector.connect(ConnectionIP);
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
	
	public void create(String path, String data) throws KeeperException, InterruptedException, IllegalStateException, IOException
	{
		byte [] dataByte = data.getBytes();
		//Persistent znode - stay in ensemble until client closes it
		//Ephemeral znode  - temporary
		m_oZooKeeper.create(path, dataByte, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
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

	public void getData(String string, boolean b, DataCallback taskDataCallback, String task) {
		// TODO Auto-generated method stub
		m_oZooKeeper.getData("/tasks/" + task, false, taskDataCallback, task);
	}
}
	
