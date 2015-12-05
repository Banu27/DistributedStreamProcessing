package edu.uiuc.cs425;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
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
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;

public class ComponentManager implements Runnable {

	private List<String> 							m_lWorkersList;
	private HashMap<String, CommandIfaceProxy> 		m_hProxyCache;
	private Lock									m_WorkerListLock;
    private HashMap<String,ArrayList<String>>       m_IPtoTaskString;
	private int 									m_nNextAssignableWorker;
	private HashMap<String, Topology> 				m_hTopologyList;
	private Logger 									m_oLogger;
	private int 									m_nCommandServicePort;
	private String 									m_sJarFilesDir;
	private ZooKeeperWrapper 						m_oZooKeeperWrapper;
	private String 									m_sZooKeeperConnectionIP;
	private ConfigAccessor 							m_oConfig;

	// Methods

	public ComponentManager() {
		m_lWorkersList = new ArrayList<String>();
		m_WorkerListLock = new ReentrantLock();
		m_hProxyCache = new HashMap<String, CommandIfaceProxy>();
		m_IPtoTaskString = new HashMap<String, ArrayList<String>>();
	}

	public void Initialize(ConfigAccessor oAccess, Logger oLogger) {
		m_oConfig = oAccess;
		m_nNextAssignableWorker = 0;
		m_oZooKeeperWrapper = new ZooKeeperWrapper();
		m_sZooKeeperConnectionIP = m_oConfig.ZookeeperIP();
		m_oLogger = oLogger;
		m_oZooKeeperWrapper.Initialize(m_sZooKeeperConnectionIP, m_oLogger);
		m_sJarFilesDir = m_oConfig.JarPath();
		m_nCommandServicePort = m_oConfig.CmdPort();
		try {
			m_oZooKeeperWrapper.create(new String("/Topologies"), new String("Consists of all topologies"),
					createNodeCallback);
			m_oZooKeeperWrapper.create(new String("/Workers"), new String("Consists of all workers"),
					createNodeCallback);
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

	/*
	public void Initialize(String jarFileDir, String ZKConnectionIP, Logger oLogger, ConfigAccessor oConfig,
			ZooKeeperWrapper oZooKeeper) {
		m_oConfig = oConfig;
		m_nNextAssignableWorker = 0;
		m_sZooKeeperConnectionIP = ZKConnectionIP;
		m_oZooKeeperWrapper = oZooKeeper;
		m_oLogger = oLogger;
		// This needs to be assigned
		m_sJarFilesDir = jarFileDir;
		try {
			m_oZooKeeperWrapper.create(new String("/Topologies"), new String("Consists of all topologies"),
					createNodeCallback);
			m_oZooKeeperWrapper.create(new String("/Workers"), new String("Consists of all workers"),
					createNodeCallback);
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
 */
	public void AdmitNewWorker(String IP) // (Thrift)
	{
		// m_lWorkersList.add(new String(IP + String.valueOf(GetMyLocalTime())))
		m_WorkerListLock.lock();
		m_lWorkersList.add(IP);
		m_WorkerListLock.unlock();

		// Zookeeper update with new worker
		try {
			m_oZooKeeperWrapper.create(new String("/Workers/" + IP), IP, createNodeCallback);
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
				 * Try again. Note that registering again is not a problem. If
				 * the znode has already been created, then we get a NODEEXISTS
				 * event back.
				 */
				// createParent(path, (byte[]) ctx);
				// MIGHT NEED TO FILL THIS
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

	
	ByteBuffer GetJar(String topology)
	{
		String file = m_sJarFilesDir + "/" + topology + ".jar";
		FileInputStream fIn;
	    FileChannel fChan;
	    long fSize;
	    ByteBuffer mBuf = null;

	    try {
	      fIn = new FileInputStream(file);
	      fChan = fIn.getChannel();
	      fSize = fChan.size();
	      mBuf = ByteBuffer.allocate((int) fSize);
	      fChan.read(mBuf);
	      mBuf.rewind();
	      fChan.close(); 
	      fIn.close(); 
	    } catch (IOException exc) {
	      System.out.println(exc);
	      System.exit(1);
	    }
	    
	    return mBuf;
	}
	
	
	public int getWorkersSize() {
		
		if (m_lWorkersList.isEmpty()) {
			return 0;
		} else {
			return m_lWorkersList.size();
		}
	}

	Watcher workersChangeWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if (e.getType() == EventType.NodeChildrenChanged) {
				m_oLogger.Info("Evenet Path is : " + e.getPath());
				assert"/Workers".equals(e.getPath());
				getWorkers();
			}
		}
	};

	void getWorkers() {
		m_oZooKeeperWrapper.getChildren("/Workers", workersChangeWatcher, workersGetChildrenCallback, null);
	}

	ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getWorkers();
				break;
			case OK:
				m_oLogger.Info("Succesfully got a list of workers: " + children.size() + " workers");
				reassignAndSet(children);
				break;
			default:
				m_oLogger.Error("getChildren failed" + path);
			}
		}
	};

	void reassignAndSet(List<String> children) {
		
		m_WorkerListLock.lock();
		m_oLogger.Info("Removing existing workers and resetting");
		m_lWorkersList.clear();
		for (String worker : children) {
			m_lWorkersList.add(worker);
			m_oLogger.Info("Added child : " + worker);
		}
		m_WorkerListLock.unlock();
		
	}	

	public int WriteFileIntoDir(ByteBuffer file, String filename) {
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(filename);
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

	public void ReceiveNewJob(ByteBuffer file, String topologyName) {
		// Client has to give the file here after converting to bytebuffer
		String filename = m_sJarFilesDir + "/" + topologyName + ".jar";
		WriteFileIntoDir(file, filename);
		RetrieveTopologyComponents(filename, topologyName);
		// Send jars to all the workers.
		StartComponentsAtNodes(topologyName, filename);
	}

	void StartComponentsAtNodes(String TopologyName, String pathToJar) {
		Topology Components = m_hTopologyList.get(TopologyName);

		try {
			Set<String> ComponentNames = Components.GetKeys();
			Iterator<String> iterator = ComponentNames.iterator();
			m_WorkerListLock.lock();
			while (iterator.hasNext()) {
				String compName = iterator.next();
				for (int i = 0; i < Components.GetParallelismLevel(compName); i++) {
					// Call the start task at the worker
					CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
					int nodeIndex = m_nNextAssignableWorker % m_lWorkersList.size();
					m_nNextAssignableWorker++;
					if (Commons.SUCCESS == ProxyTemp.Initialize(
							m_lWorkersList.get(nodeIndex),
							m_nCommandServicePort, m_oLogger)) {
						ProxyTemp.CreateTask(compName, TopologyName, i);
						String taskString = TopologyName + ":" + compName + ":" + Integer.toString(i);
						if( m_IPtoTaskString.containsKey(m_lWorkersList.get(nodeIndex)))
						{
							m_IPtoTaskString.get(m_lWorkersList.get(nodeIndex)).add(taskString);
						} else {
							ArrayList<String> newList = new ArrayList<String>();
							newList.add(taskString);
							m_IPtoTaskString.put(m_lWorkersList.get(nodeIndex), newList);
						}
					}

				}

			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			m_WorkerListLock.unlock();
		}
	}

	private void RetrieveTopologyComponents(String pathToJar, String TopologyName) // (Thrift)
	{
		// Get the topology from the jar.
		// Receive the parallelism level
		// Do Round Robin
		try {
			URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
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



	// private long GetMyLocalTime()
	// {
	// return new Date().getTime();
	// }

	private void ReSched(String sIP) {
		// remove from zookeeper
		deleteZnode("/Workers/" + sIP);
		// remove all the topology nodes
		ArrayList<String> list_ = m_IPtoTaskString.get(sIP);
		if(list_ != null)
		{
			for(int i=0; i<list_.size(); ++i)
			{
				String zNode = "/Topologies/" + list_.get(i);
				deleteZnode(zNode);
			}
		}
		
		// resched all the components from the failed node to the new node
		if(list_ != null)
		{
			for(int i=0; i<list_.size(); ++i)
			{
				String[] tokens = list_.get(i).split(":");
				CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
				int nodeIndex = m_nNextAssignableWorker % m_lWorkersList.size();
				m_nNextAssignableWorker++;
				if (Commons.SUCCESS == ProxyTemp.Initialize(
						m_lWorkersList.get(nodeIndex),
						m_nCommandServicePort, m_oLogger)) {
					try {
						ProxyTemp.CreateTask(tokens[1], tokens[0], Integer.parseInt(tokens[2]));
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					if( m_IPtoTaskString.containsKey(m_lWorkersList.get(nodeIndex)))
					{
						m_IPtoTaskString.get(m_lWorkersList.get(nodeIndex)).add(list_.get(i));
					} else {
						ArrayList<String> newList = new ArrayList<String>();
						newList.add(list_.get(i));
						m_IPtoTaskString.put(m_lWorkersList.get(nodeIndex), newList);
					}
				}
			}
		}
		m_IPtoTaskString.remove(sIP);
	}

	private void CheckIfWorkersAreAlive() {
		ListIterator<String> iter = m_lWorkersList.listIterator();
		while (iter.hasNext()) {
			String sIP = iter.next();
			// make is alive call
			if (m_hProxyCache.containsKey(sIP)) {
				if (!m_hProxyCache.get(sIP).isAlive()) {
					// remove entries from worklist and cache
					iter.remove();
					m_hProxyCache.remove(sIP);
					// if false do rescheduling
					ReSched(sIP);
				}
			}
		}
	}

	// public void cleanZK()
	// {
	// if(m_oZooKeeperWrapper.exists("/Workers"))
	// deleteZnode("/Workers");

	// }

	public void deleteZnode(String node) {
		if (m_oZooKeeperWrapper.GetChildren(node).isEmpty()) {
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

		for (String child : children) {
			deleteZnode(child);
		}

	}

	public void run() {
		CheckIfWorkersAreAlive();
		try {
			Thread.sleep(m_oConfig.FailureInterval());
		} catch (InterruptedException e1) {

		}
	}

	// this method starts the streaming master
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage: java -cp ~/distStreamProcessing.jar edu.uiuc.cs425.NodeManager <xml_path>");
			//System.exit(Commons.FAILURE);
		}
		///Users/banumuthukumar/Desktop/cs425/MP4/DistributedStreamProcessing/distStreamProcessing/config.xml
		//String sXML = args[0];
		String sXML = "/Users/banumuthukumar/Desktop/cs425/MP4/DistributedStreamProcessing/distStreamProcessing/config.xml";
		final ConfigAccessor m_oConfig = new ConfigAccessor();

		// instantiate logger and config accessor
		if (Commons.FAILURE == m_oConfig.Initialize(sXML)) {
			System.out.println("Failed to Initialize XML");
			System.exit(Commons.FAILURE);
		}

		final Logger m_oLogger = new Logger();
		if (Commons.FAILURE == m_oLogger.Initialize(m_oConfig.LogPath())) {
			System.out.println("Failed to Initialize logger object");
			System.exit(Commons.FAILURE);
		}
		
		m_oLogger.Info("Config accessor and logger initialized successfully!");
		
		String hostIP = null;
		try {
			hostIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e1));
		}

		// check if the host ip is same as the master ip
		if (!hostIP.equals(m_oConfig.MasterIP())) {
			System.out.println("host IP not same as the master IP in ");
			System.exit(Commons.FAILURE);
		}
		
		
		// init component manager
		ComponentManager compManager = new ComponentManager();
		compManager.Initialize(m_oConfig, m_oLogger);

		m_oLogger.Info("Component Manager is up");
		// start the thrift service

		final CommandIfaceImpl m_oCommandImpl = new CommandIfaceImpl();
		m_oCommandImpl.Initialize(compManager, null);
		
		Thread 					m_oCmdServThread;
		
		m_oCmdServThread = new Thread(new Runnable() {           
            public void run() { 
            	try {
            		TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(m_oConfig.CmdPort());
            		TNonblockingServer.Args args = new TNonblockingServer.Args(serverTransport).processor(new CommandInterface.Processor(m_oCommandImpl));
        		    
        		    args.transportFactory(new TFramedTransport.Factory(104857600)); //100mb
        		    TServer server = new TNonblockingServer(args);
        		    
        		    server.serve();
        		} catch (TException e)
        		{
        			m_oLogger.Error(m_oLogger.StackTraceToString(e));
        			System.exit(Commons.FAILURE);
        		}
        		return;
        	} 
        });
		
		m_oLogger.Info("CommandImpl set up");
		m_oCmdServThread.start();
		
		// start the master thread that looks ping-acks the workers
		Thread m_FailureDetThread = new Thread(compManager);
		m_FailureDetThread.start();
		
		// wait on the threads
		try {
			m_oCmdServThread.join();
			m_FailureDetThread.join();
		} catch (InterruptedException e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			System.exit(Commons.FAILURE);
		}
	}

}
