package edu.uiuc.cs425;

import java.io.File;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.thrift.TException;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.w3c.dom.Node;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;

public class NodeManager implements Runnable{

	// key: "<JobName>:<Component>:<Instance>" -> Task
	private HashMap<String, TaskManager> 			m_hTaskMap;
	
	// key: "<JobName>:<Component>:<Instance>" -> NodeIP
	// This map will be updated by the watch registered with
	// the zookeeper
	private HashMap<String,String> 					m_hClusterInfo;
	private Lock 									m_hClusterInfoLock;
	
	private HashMap<String,Topology> 				m_hTopologyList;

	private String 									m_sJarFilesDir;
	private String 									m_sMyIp;

	// list of all the worker currently in the cluster
	private ArrayList<String> 						m_lWorkerIPs;

	// data structure to store the tuples to be transferred to other nodes
	// a thread will go through them every interval and transfer them to other
	// nodes. We want to tansfer as a batch
	private HashMap<String, Queue<Tuple>> 			m_OutputTupleBucket;
	private int	 									m_nTransferInterval;
	private Lock 									m_oMutexOutputTuple;

	// these are the node level input and output queues. The
	// handlers and producers for the disruptor are implemented
	// within the class. Different init methods needs to be called
	// for each of the disruptor type.
	private DisruptorWrapper 						m_oInputTupleQ;
	private DisruptorWrapper 						m_oOutputTupleQ;

	// hash map for ip to commandinterface proxy. Idea is to cache the
	// proxies and not to recreate them every time
	private HashMap<String, CommandIfaceProxy> 		m_hIPtoProxy;

	private Logger 									m_oLogger;
	private ConfigAccessor 							m_oConfig;
	private ZooKeeperWrapper						m_oZooKeeper;
	private String									m_sZooKeeperConnectionIP;
	private String									m_sNodeIP;
	
	// master 
	private CommandIfaceProxy                       m_oMasterProxy;

	public NodeManager() {
		m_hTaskMap 					= new HashMap<String, TaskManager>();
		m_hClusterInfo 				= new HashMap<String, String>();
		m_hTopologyList 			= new HashMap<String, Topology>();
		m_OutputTupleBucket         = new HashMap<String, Queue<Tuple>>();
		m_oMutexOutputTuple 		= new ReentrantLock(true);
		m_hClusterInfoLock          = new ReentrantLock(true);
		try {
			m_sMyIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {

		}
		m_lWorkerIPs = new ArrayList<String>();
		m_oZooKeeper = new ZooKeeperWrapper();
		m_oMasterProxy = new CommandIfaceProxy();
		m_hIPtoProxy =  new HashMap<String, CommandIfaceProxy>();
	}


	public void Initialize(Logger logger, ConfigAccessor config, String NodeIP) {

		m_oLogger = logger;
		m_oConfig = config;
		m_oInputTupleQ = new DisruptorWrapper(m_oConfig.RingBufferValue());
		m_oOutputTupleQ = new DisruptorWrapper(m_oConfig.RingBufferValue());
		m_oInputTupleQ.InitNodeInput(this);
		m_oOutputTupleQ.InitNodeOutput(this);

		m_sJarFilesDir = m_oConfig.JarPath();
		m_sZooKeeperConnectionIP = m_oConfig.ZookeeperIP();
		m_sNodeIP = NodeIP;
		m_oZooKeeper.Initialize(m_sZooKeeperConnectionIP, m_oLogger);;
		//Assuming ComponentManager/Master is already up.
		
		System.out.println("Zookeeper setup");
		
		m_oZooKeeper.getChildren("/Topologies", TopologyChangeWatcher, TopologyGetChildrenCallback, null);
		m_nTransferInterval = m_oConfig.TupleTransferInterval(); // should be around 100ms
		
		System.out.println("Trying to set up master proxy");
		m_oMasterProxy.Initialize(m_oConfig.MasterIP(), m_oConfig.CmdPort(), m_oLogger);
	}
	
	public void RequestMasterAddWorker()
	{
		try {
			m_oMasterProxy.AddWorker(m_sMyIp);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private int WriteFileIntoDir(ByteBuffer file, String filename) {
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
	
	public void UpdateClusterInfo(String zNodePath)
	{
		try {
			String data = m_oZooKeeper.read(zNodePath);
			String [] tokens = zNodePath.split("/");
			String compChild = tokens[tokens.length-1];
			m_hClusterInfoLock.lock();
			m_hClusterInfo.put(compChild, data);
			m_hClusterInfoLock.unlock();
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	public void UpdateClusterInfo()
	{
		// key: "<JobName>:<Component>:<Instance>" -> NodeIP
		// This map will be updated by the watch registered with
		// the zookeeper
		
		m_oLogger.Info("Updating cluster info");
		m_hClusterInfoLock.lock();
		m_hClusterInfo.clear();
		String zNodePath = "/Topologies";
		
		List<String> children = m_oZooKeeper.GetChildren(zNodePath);
		
		for(String child : children)
		{
			try {
			
				m_hClusterInfo.put(child, m_oZooKeeper.read("/Topologies/" + child));
			
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				continue;
			} catch (KeeperException e) {
				e.printStackTrace();
				continue;
			} catch (InterruptedException e) {
				e.printStackTrace();
				continue;
			}
		}
		m_hClusterInfoLock.unlock();
	}	
	
	Watcher TopologyChangeWatcher = new Watcher() 
	{
		public void process(WatchedEvent e) {
			if(e.getType() == EventType.NodeChildrenChanged) {
				m_oLogger.Info("Evenet Path is : " + e.getPath());
				assert "/Topologies".equals( e.getPath() );
				getComponents();
			}
		}
	};

	void getComponents()
	{
		m_oZooKeeper.getChildren("/Topologies", 
				TopologyChangeWatcher, 
				TopologyGetChildrenCallback, 
				null);
	}

	ChildrenCallback TopologyGetChildrenCallback = new ChildrenCallback() 
	{
		public void processResult(int rc, String path, Object ctx, List<String> children){
			switch (Code.get(rc)) { 
			case CONNECTIONLOSS:
				getComponents();
				break;
			case OK:
				m_oLogger.Info("Succesfully got a list of topologies: " 
						+ children.size() 
						+ " topologies");
				UpdateClusterInfo();
				break;
			default:
				m_oLogger.Error("getChildren failed" + path);
			}
		}
	};
	
	public void StartTopology(String sTopology)
	{
		for (String key_ : m_hTaskMap.keySet()) {
			if(key_.contains(sTopology))
			{
				m_hTaskMap.get(key_).Start();
			}
		}

	}

	public void CreateTask(String compName, int instanceId, String topologyName, String fullClassName) {
		try {
		
			// check if file is available, else request file from master
			m_oLogger.Info("Creating task in NM");
			String pathToJar = m_sJarFilesDir + "/" + topologyName  +".jar";
			File f = new File(pathToJar);
			if(!f.exists()) { 
				ByteBuffer buf = null;
				try {
					buf = m_oMasterProxy.GetJarFromMaster(topologyName);
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				WriteFileIntoDir(buf,pathToJar);
			}
			RetrieveTopologyComponents(pathToJar, topologyName,fullClassName);
			URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);
			//String[] topologyZkName = topologyName.split("/");
			//topologyName = topologyName.replace('/', '.');
			m_oLogger.Info("Retrieving topology : " + topologyName);
			System.out.println("Retrieving topology : " + topologyName);
			Topology componentsTopology = m_hTopologyList.get(topologyName);
			String topologyZkname = componentsTopology.sTopologyName;
			if(componentsTopology.IsValid())
			{	System.out.println("Valid topology found");
				String classname = componentsTopology.Get(compName).getClassName();
				m_oLogger.Info("Class name retrieved : " + classname);
				Class<?> componentClass = cl.loadClass(classname);
			
				// there are two possible components - spout and bolt
				TaskManager task = new TaskManager();
				System.out.println("topology name is : " + topologyZkname);
				String key_ = topologyZkname + ":" + compName + ":" + Integer.toString(instanceId);
				m_hTaskMap.put(key_, task);
				if(m_hTopologyList.get(topologyName).Get(compName).getCompType() == Commons.BOLT)
				{
					IBolt bolt = (IBolt) componentClass.newInstance();
					task.Init(topologyName, compName, instanceId, bolt, this);
				}
				else
				{
					ISpout spout = (ISpout) componentClass.newInstance();
					task.Init(topologyName, compName, instanceId, spout, this);
				
				}
				m_oZooKeeper.create("/Topologies/" + key_,m_sNodeIP,createNodeCallback);
				m_oZooKeeper.getData("/Topologies/" + key_, ComponentDataChangeWatcher, ComponentDataChangeCallback, null);
			}
			else
			{
				m_oLogger.Error("Couldn't find topology!! ");
			}

		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	
	
	//For change of any component instance or addition of new instance
	Watcher ComponentDataChangeWatcher = new Watcher(){ 
	    public void process(WatchedEvent e) {
	        if(e.getType() == EventType.NodeDataChanged) {
	            getData(e.getPath());
	        }
	    }
	};

	void getData(String zNodePath) 
	{
	    m_oZooKeeper.getData(zNodePath,
	                   ComponentDataChangeWatcher,
	                   ComponentDataChangeCallback,
	                   null); 
	}

	DataCallback ComponentDataChangeCallback = new DataCallback() 
	{
	    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) 
	    {
	        switch(Code.get(rc)) {
	        case CONNECTIONLOSS:
	            //FILL?

	            break;
	        case OK:
	        	UpdateClusterInfo(path);	
	            break;
	        default:
	            m_oLogger.Error("getChildren failed." + path);
	        }
	    }

	};

	
	// this call is made from the task manager after a new tuple is emitted.
	// the task ref is here in case we need it to get some other information
	public void ReceiveProcessedTuple(Tuple tuple, TaskManager task) {

		// add the source information to the tuple
		tuple.SetSrcFieldInfo(task.m_sJobname, task.m_sComponentName, task.m_nInstanceId);
		// add to disruptor queue
		//System.out.println("Received tuple from task " + task.m_sJobname + ":" +
				//task.m_sComponentName + ":" + Integer.toString(task.m_nInstanceId));
		m_oOutputTupleQ.WriteData(tuple);
	}

	// AddTask(String compName, String jobname, int instanceId)
	// ReceiveProcessedTuple(Tuple, pointerToTaskMan)
	// StartJob(String jobname) //This is because we cannot start it in AddTask
	// - wait to add all the tasks)

	@SuppressWarnings("unchecked")
	private void RetrieveTopologyComponents(String pathToJar, String TopologyName, String fullClassName) // (Thrift)
	{
		// Get the topology from the jar.
		// Receive the parallelism level
		// Do Round Robin
		try {
			URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			fullClassName = fullClassName.replace('/', '.');
			Class<?> topologyClass = cl.loadClass(fullClassName);
			Object topologyObject = topologyClass.newInstance();

			Method createTopology = topologyClass.getMethod("CreateTopology");
			Topology components = (Topology) createTopology.invoke(topologyObject);
			components.setJarFilepath(pathToJar);
			if(components.IsValid())
			{
				m_hTopologyList.put(TopologyName,components );
				m_oLogger.Info("Put topology in hash : " + TopologyName);
			}
			else
				m_oLogger.Error("No topology object retrieved");

		} catch (IOException e1) {
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
		} catch (NoSuchMethodException e) {
			System.out.println("Create Topology method not present. Aborting!!");
			// TODO Auto-generated catch block
			e.printStackTrace();
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

	}

	// code to identify the next component
	public ArrayList<Tuple> IdentifyNextComponents(Tuple tuple) {
		
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		// get the source component
		String sSrc = tuple.m_sSrcCompName;
		// get the next comp list
		Topology topo = m_hTopologyList.get(tuple.m_sJobname);
		ArrayList<String> nextComp = topo.GetNextComponents(sSrc);
		if(nextComp!= null && nextComp.size() > 0)
		{
			// assign dest to the first tuple
			String firtComp = nextComp.get(0);
			int destGrouping = topo.Get(firtComp).getGroupingType();
			int inst = 0;
			if( destGrouping == Commons.GROUPING_FIELD)
			{
				String sFieldType = topo.Get(firtComp).getFieldGroup();
				String sFieldValue = tuple.GetStringValue(sFieldType);
				int hash = Commons.Hash(sFieldValue);
				inst = hash%topo.Get(firtComp).getParallelismLevel();
				//System.out.println("Grouping_field inst " + Integer.toString(inst) );
				
			} else {
				inst = topo.Get(firtComp).nextTupleIndex() % 
						topo.Get(firtComp).getParallelismLevel();
			}
			tuple.m_nDestInstId = inst;
			tuple.m_sDestCompName = firtComp;
			tuples.add(tuple);
			// now go through the rest of the next comp
			for(int i = 1; i < nextComp.size(); ++i)
			{
				Tuple newTuple = new Tuple();
				tuple.Copy(newTuple);
				String nextCompName = nextComp.get(i);
				destGrouping = topo.Get(nextCompName).getGroupingType();
				inst = 0;
				if( destGrouping == Commons.GROUPING_FIELD)
				{
					String sFieldType = topo.Get(nextCompName).getFieldGroup();
					String sFieldValue = tuple.GetStringValue(sFieldType);
					int hash = Commons.Hash(sFieldValue);
					inst = hash%topo.Get(nextCompName).getParallelismLevel();
					
				} else {
					inst = topo.Get(nextCompName).nextTupleIndex() % 
							topo.Get(nextCompName).getParallelismLevel();
				}
				tuple.m_nDestInstId = inst;
				tuple.m_sDestCompName = nextCompName;
				tuples.add(tuple);
			}
		}
		
		return tuples;
	}

	public String GetNextNode(Tuple tuple) {
		// look at the job, component and instance info in
		// the tuple and select the node
		String key = tuple.m_sJobname + ":" + tuple.m_sDestCompName + ":" + 
				Integer.toString(tuple.m_nDestInstId);
		String sIP = null;
		m_hClusterInfoLock.lock();
		if(m_hClusterInfo.containsKey(key))
		{
			sIP = m_hClusterInfo.get(key);
		} else
		{
			m_oLogger.Error("Unable to find the IP containing the instance " + key + ". Tuple "
						+ "will not proceed to next components" );
		} 
		m_hClusterInfoLock.unlock();
		return sIP;
	}

	// this call is made by the output disruptor handler
	public void SendToNextComponent(Tuple tuple) {

		ArrayList<Tuple> tuples = IdentifyNextComponents(tuple);

		m_oMutexOutputTuple.lock();
		for (int i = 0; i < tuples.size(); ++i) {
			String sIP = GetNextNode(tuples.get(i));
			//System.out.println("next destination of tuple is " + sIP);
			if(sIP == null) 
			{
				System.out.println("NULL");
				continue;
			}
			Queue<Tuple> queue = m_OutputTupleBucket.get(sIP);
			if (queue == null)
				m_OutputTupleBucket.put(sIP, new LinkedList<Tuple>());
			queue = m_OutputTupleBucket.get(sIP);
			queue.add(tuple);
		}
		m_oMutexOutputTuple.unlock();
	}

	// this call is made by the input disruptor handler
	public void SendTupleToTask(Tuple tuple) {
		//look at the destination fields in the tuple and decide which 
		//task should get it
		String key = tuple.m_sJobname + ":" + tuple.m_sDestCompName + ":" + 
							Integer.toString(tuple.m_nDestInstId);
		if(m_hTaskMap.containsKey(key))
		{
			TaskManager mgr = m_hTaskMap.get(key);
			mgr.AddTuple(tuple);
		} else
		{
			m_oLogger.Error("Tuple send to wrong node. Tuple will not move forward");
		}	
	}

	public void ReceiveTuplesFromOutside(List<ByteBuffer> tuples)
	{
		// add tuples to the disruptor queue
		for(ByteBuffer buf : tuples)
		{
			// TODO: Need to find better way with out creating extra memory
			byte[] bufArr = new byte[buf.remaining()];
			buf.get(bufArr);
			Tuple tuple = (Tuple)SerializationUtils.deserialize(bufArr);
			m_oInputTupleQ.WriteData(tuple);
		}
	}

	// called
	public void run() {
		try {
			Thread.sleep(m_nTransferInterval);
		} catch (InterruptedException e1) {
		}
		while (true) {
			m_oMutexOutputTuple.lock();

			// get the keys
			Set<String> sIPs = m_OutputTupleBucket.keySet();
			if( sIPs != null) {
				// iterate through the keysq
				for (String sIP : sIPs) {
					// send list of tuples to the other node
					Queue<Tuple> queue = m_OutputTupleBucket.get(sIP);
					List<ByteBuffer> serTuples = new ArrayList<ByteBuffer>();
					while (queue.size() > 0) {
						serTuples.add(ByteBuffer.wrap(queue.peek().Serialize()));
						queue.remove();
					}

					// check if it is the same node and directly add to input
					// disruptor
					if (sIP.equals(m_sMyIp)) {
						ReceiveTuplesFromOutside(serTuples);
					} else {
						// call proxy to send the tuples
						CommandIfaceProxy prxy = null;
						if (m_hIPtoProxy.containsKey(sIP)) {
							prxy = m_hIPtoProxy.get(sIP);
						} else {
							prxy = new CommandIfaceProxy();
							if (Commons.FAILURE == prxy.Initialize(sIP, m_oConfig.CmdPort(), m_oLogger)) {
								m_oLogger.Error("unable to connect to worker to send tuples " + sIP);
								continue;
							}
							m_hIPtoProxy.put(sIP, prxy);
						}
						try {
							prxy.TransferTupleToNode(serTuples.size(), serTuples);
						} catch (TException e) {
							e.printStackTrace();
						}
					}
				}
			}

			m_oMutexOutputTuple.unlock();

			try {
				Thread.sleep(m_nTransferInterval);
			} catch (InterruptedException e1) {

			}
		}
	}

	// this method starts the streaming worker
	public static void main( String[] args )
    {
		if( args.length !=1 )
		{
			System.out.println("Usage: java -cp ~/distStreamProcessing.jar edu.uiuc.cs425.NodeManager <xml_path>");
			
			//Commented for local machine test on eclipse with cmd line args
			System.exit(Commons.FAILURE);
		}
		String sXML = args[0];
		//String sXML = "/Users/banumuthukumar/Desktop/cs425/MP4/DistributedStreamProcessing/distStreamProcessing/config.xml";
		final ConfigAccessor m_mConfig = new ConfigAccessor();
		
		// instantiate logger and config accessor
		if( Commons.FAILURE == m_mConfig.Initialize(sXML))
		{
			System.out.println("Failed to Initialize XML");
			System.exit(Commons.FAILURE);
		}
		
		final Logger m_mLogger = new Logger();
		if( Commons.FAILURE == m_mLogger.Initialize(m_mConfig.LogPath()))
		{
			System.out.println("Failed to Initialize logger object");
			System.exit(Commons.FAILURE);
		}
		
		System.out.println("Config accessor and logger initialized successfully!");
		
		// instantiate a new copy of the node manager
		// 
		String hostIP = null;
		try {
			hostIP  = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			m_mLogger.Error(m_mLogger.StackTraceToString(e1));
		}
		NodeManager m_mNodeManager = new NodeManager();
		
		System.out.println("Host IP : " + hostIP);
		
		m_mNodeManager.Initialize(m_mLogger, m_mConfig, hostIP);
		
		System.out.println("Node Manager up ");
		
		m_mNodeManager.RequestMasterAddWorker();
		
		// instantiate the thrift server 
		
		final CommandIfaceImpl m_mCommandImpl = new CommandIfaceImpl();
		m_mCommandImpl.Initialize(null, m_mNodeManager);
		Thread 					m_oCmdServThread;
		m_oCmdServThread = new Thread(new Runnable() {           
            public void run() { 
            	try {
            		TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(m_mConfig.CmdPort());
            		TNonblockingServer.Args args = new TNonblockingServer.Args(serverTransport).processor(new CommandInterface.Processor(m_mCommandImpl));
        		    
        		    args.transportFactory(new TFramedTransport.Factory(104857600)); //100mb
        		    TServer server = new TNonblockingServer(args);
        		    
        		    server.serve();
        		} catch (TException e)
        		{
        			m_mLogger.Error(m_mLogger.StackTraceToString(e));
        			System.exit(Commons.FAILURE);
        		}
        		return;
        	} 
        });
		m_oCmdServThread.start();
		
		// start the tuple transfer thread
		Thread m_TupleTransferThread = new Thread(m_mNodeManager);
		m_TupleTransferThread.start();
		
		// wait for these threads to join
		try {
			m_oCmdServThread.join();
			m_TupleTransferThread.join();
		} catch (InterruptedException e) {
			m_mLogger.Error(m_mLogger.StackTraceToString(e));
			System.exit(Commons.FAILURE);
		}
		
		
		
    }
}
