package edu.uiuc.cs425;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;

public class ComponentManager implements Runnable{

		
	private	List<String>								m_lWorkersList;
	private HashMap<String, CommandIfaceProxy>          m_hProxyCache;
	Lock												m_WorkerListLock;
	
	private int											m_nNextAssignableWorker;
	private HashMap<String, Topology>			 		m_hTopologyList;
	private Logger										m_oLogger;
	private int											m_nCommandServicePort;
	private String										m_sJarFilesDir;
	private ZooKeeperWrapper							m_oZooKeeperWrapper;
	private String										m_sZooKeeperConnectionIP;
	private ConfigAccessor								m_oConfig;
	
	//Methods

	public ComponentManager()
	{
		m_lWorkersList   = new ArrayList<String>();
		m_WorkerListLock = new ReentrantLock();
		m_hProxyCache    = new HashMap<String, CommandIfaceProxy>();
	}
	
	public void Initialize(String ZKConnectionIP, Logger oLogger)
	{
		m_oZooKeeperWrapper = new ZooKeeperWrapper();
		m_sZooKeeperConnectionIP = ZKConnectionIP;
		m_oLogger = oLogger;
		m_oZooKeeperWrapper.Initialize(m_sZooKeeperConnectionIP, m_oLogger);
		
		try {
			m_oZooKeeperWrapper.create(new String("/Topologies"), new String("Consists of all topologies"),createNodeCallback);
			m_oZooKeeperWrapper.create(new String("/Workers"), new String("Consists of all workers"),createNodeCallback);
			m_oZooKeeperWrapper.getChildren("/Workers", workersChangeWatcher, workersGetChildrenCallback, null);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void Initialize(String jarFileDir, String ZKConnectionIP, Logger oLogger, ConfigAccessor oConfig, ZooKeeperWrapper oZooKeeper)
	{
		m_oConfig = oConfig;
		m_nNextAssignableWorker = 0;
		m_sZooKeeperConnectionIP = ZKConnectionIP;
		m_oZooKeeperWrapper = oZooKeeper;
		m_oLogger = oLogger;
		//This needs to be assigned	
		m_sJarFilesDir = jarFileDir;
		try {
			m_oZooKeeperWrapper.create(new String("/Topologies"), new String("Consists of all topologies"),createNodeCallback);
			m_oZooKeeperWrapper.create(new String("/Workers"), new String("Consists of all workers"),createNodeCallback);
			m_oZooKeeperWrapper.getChildren("/Workers", workersChangeWatcher, workersGetChildrenCallback, null);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void AdmitNewWorker(String IP) //(Thrift)
	{
		//m_lWorkersList.add(new String(IP + String.valueOf(GetMyLocalTime())))
		m_lWorkersList.add(IP);
		
		//Zookeeper update with new worker
		try {
			m_oZooKeeperWrapper.create(new String("/Workers/"+IP),IP,createNodeCallback);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	StringCallback createNodeCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                //createParent(path, (byte[]) ctx);
                //MIGHT NEED TO FILL THIS
                break;
            case OK:
                m_oLogger.Info("Created node");
                
                break;
            case NODEEXISTS:
                m_oLogger.Warning("ZNode already registered: " + path);
                
                break;
            default:
                m_oLogger.Error("Something went wrong: " + path);
            }
        }
    };
	
	public int getWorkersSize()
	{
		if(m_lWorkersList.isEmpty()) {
			return 0;
		} else {
			return m_lWorkersList.size();
		}
	}

	Watcher workersChangeWatcher = new Watcher() 
	{
		public void process(WatchedEvent e) {
			if(e.getType() == EventType.NodeChildrenChanged) {
				m_oLogger.Info("Evenet Path is : " + e.getPath());
				assert "/Workers".equals( e.getPath() );
				getWorkers();
			}
		}
	};

	void getWorkers()
	{
		m_oZooKeeperWrapper.getChildren("/Workers", 
				workersChangeWatcher, 
				workersGetChildrenCallback, 
				null);
	}

	ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() 
	{
		public void processResult(int rc, String path, Object ctx, List<String> children){
			switch (Code.get(rc)) { 
			case CONNECTIONLOSS:
				getWorkers();
				break;
			case OK:
				m_oLogger.Info("Succesfully got a list of workers: " 
						+ children.size() 
						+ " workers");
				reassignAndSet(children);
				break;
			default:
				m_oLogger.Error("getChildren failed" + path);
			}
		}
	};


	void reassignAndSet(List<String> children)
	{
		List<String> toProcess;

		if(m_lWorkersList.isEmpty()) 
		{
			m_lWorkersList = children;
			toProcess = null;
		} 

		else 
		{
			m_oLogger.Info( "Removing existing workers and resetting" );
			m_lWorkersList.clear();
			for(String worker : children)
			{
				m_lWorkersList.add(worker);
				m_oLogger.Info("Added child : " + worker);
			}
			toProcess = m_lWorkersList;
			
		}

		if(toProcess != null) {

			ReassignAbsentWorkerTasks();
		}
	}

	private void ReassignAbsentWorkerTasks() {
		// TODO Auto-generated method stub

	}
	
	public int WriteFileIntoDir(ByteBuffer file, String filename)
	{
		FileOutputStream fos;
		try {
				fos = new FileOutputStream(m_sJarFilesDir + '/' + filename);
				WritableByteChannel channel = Channels.newChannel(fos);
				channel.write(file);
				channel.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return Commons.FAILURE;
			} catch (IOException e) {
				e.printStackTrace();
				return Commons.FAILURE;
			}
			return Commons.SUCCESS;
	}
	
	public void ReceiveNewJob(ByteBuffer file, String TopologyName, String filename)
	{
		//Client has to give the file here after converting to bytebuffer
		WriteFileIntoDir(file, filename);
		String pathToJar = m_sJarFilesDir + '/' + filename;
		RetrieveTopologyComponents(pathToJar, TopologyName);
		//Send jars to all the workers.
		StartComponentsAtNodes(TopologyName, pathToJar);
	}
	
	
	void StartComponentsAtNodes(String TopologyName, String pathToJar)
	{
		Topology Components = m_hTopologyList.get(TopologyName);
		
		try {
			Set<String> ComponentNames = Components.GetKeys();
			Iterator<String> iterator = ComponentNames.iterator();
			
			while(iterator.hasNext())
			{
				String classname = iterator.next();	
				for(int i=0; i< Components.GetParallelismLevel(classname); i++)
				{
					//Call the start task at the worker
					CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
					if(Commons.SUCCESS == ProxyTemp.Initialize(m_lWorkersList.get(m_nNextAssignableWorker), m_nCommandServicePort, m_oLogger))
					{
						ProxyTemp.CreateInstance(classname, pathToJar, i, TopologyName);
					}
					
				}
				
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void RetrieveTopologyComponents( String pathToJar, String TopologyName)  //(Thrift)
	{
		//Get the topology from the jar. 
		//Receive the parallelism level
		//Do Round Robin
		try {
			URL[] urls = { new URL("jar:file:" + pathToJar+"!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			TopologyName = TopologyName.replace('/', '.');
			Class<?> topologyClass = cl.loadClass(TopologyName);
			Object topologyObject = topologyClass.newInstance();


			Method createTopology = topologyClass.getMethod("CreateTopology");
			m_hTopologyList.put(TopologyName, (Topology) createTopology.invoke(topologyObject));
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			System.out.println("Create Topology method not present. Aborting!!");
			e.printStackTrace();
			return;
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void ScheduleJob()	//Calls start job after creating calling AddTask
	{
		//Called from Receive new job
		//Assigns component instances to nodes
		//and initializes tasks
		//Calls start task
	}
	
	//private long GetMyLocalTime()
	//{
	//	return new Date().getTime();
	//}


	private void ReSched(String sIP)
	{
		
		// remove from zookeeper
		// resched all the components from the failed node to the new node
	}
	
	private void CheckIfWorkersAreAlive()
	{
		ListIterator<String> iter = m_lWorkersList.listIterator();
		while(iter.hasNext())
		{
			String sIP = iter.next();
			// make is alive call
			if(m_hProxyCache.containsKey(sIP))
			{
				if(! m_hProxyCache.get(sIP).isAlive())
				{
					// remove entries from worklist and cache
					iter.remove();
					m_hProxyCache.remove(sIP);
					ReSched(sIP);
				}
			}
			// if false do rescheduling
		}
	}
	
	//public void cleanZK()
	//{
	//	if(m_oZooKeeperWrapper.exists("/Workers"))	
	//		deleteZnode("/Workers");
		
//	}
	
	public void deleteZnode(String node) 
	{
		if(m_oZooKeeperWrapper.GetChildren(node).isEmpty())
		{
			try {
				m_oZooKeeperWrapper.deleteZNode(node);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
		
		List<String> children = m_oZooKeeperWrapper.GetChildren("/Workers");
		
		for(String child : children)
		{
			deleteZnode(child);
		}
		
	}
	
	public void run() 
	{
		CheckIfWorkersAreAlive();
		try {
			Thread.sleep(m_oConfig.FailureInterval());
		} catch (InterruptedException e1) {

		}
	}
}





