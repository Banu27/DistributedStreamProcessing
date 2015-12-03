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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.SerializationUtils;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;

public class NodeManager implements Runnable{

	// key: "<JobName>:<Component>:<Instance>" -> Task
	private HashMap<String, TaskManager> 			m_hTaskMap;
	
	// key: "<JobName>:<Component>:<Instance>" -> NodeIP
	// This map will be updated by the watch registered with
	// the zookeeper
	private HashMap<String,String> 				m_hClusterInfo;
	
	private HashMap<String,Topology> 				m_hTopologyList;
	private HashMap<String, ClassAndInstance> 		m_hComponentInstances;

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

	public NodeManager() {
		m_hTaskMap 					= new HashMap<String, TaskManager>();
		m_hClusterInfo 				= new HashMap<String, String>();
		m_hTopologyList 			= new HashMap<String, Topology>();
		m_hComponentInstances 		= new HashMap<String, ClassAndInstance>();
		m_oMutexOutputTuple 		= new ReentrantLock(true);
		try {
			m_sMyIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {

		}
		m_lWorkerIPs = new ArrayList<String>();
	}

	public void Initialize(Logger logger, ConfigAccessor config, String ZKConnectionIP, String NodeIP, ZooKeeperWrapper oZooKeeper) {
		m_oInputTupleQ.InitNodeInput(this);
		m_oOutputTupleQ.InitNodeOutput(this);
		m_oLogger = logger;
		m_oConfig = config;
		m_sJarFilesDir = m_oConfig.JarPath();
		m_sZooKeeperConnectionIP = ZKConnectionIP;
		m_sNodeIP = NodeIP;
		m_oZooKeeper = oZooKeeper;
		//Assuming ComponentManager/Master is already up.
		
		m_oZooKeeper.getChildren("/Topologies", TopologyChangeWatcher, TopologyGetChildrenCallback, null);
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
		zNodePath = zNodePath.replace('/', ':');
		try {
			String data = m_oZooKeeper.read(zNodePath);
			if(!m_hClusterInfo.get(zNodePath).equals(data))
			{
				m_hClusterInfo.put(zNodePath, data);
			}
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
		
		m_hClusterInfo.clear();
		String zNodePath = "/Topologies";
		
		List<String> children = m_oZooKeeper.GetChildren(zNodePath);
		
		for(String child : children)
		{
			try {
			
				m_hClusterInfo.put(child, m_oZooKeeper.read("/Topologies/" + child));
			
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
				m_oLogger.Info("Succesfully got a list of workers: " 
						+ children.size() 
						+ " workers");
				UpdateClusterInfo();
				break;
			default:
				m_oLogger.Error("getChildren failed" + path);
			}
		}
	};
	
	public void ReceiveTopology(ByteBuffer jar, String topologyName) 
	{
		String pathToJar = m_sJarFilesDir + '/' + topologyName;
		WriteFileIntoDir(jar, pathToJar);
		RetrieveTopologyComponents(pathToJar, topologyName);
		// Send jars to all the workers.
	}

	public void CreateInstance(String classname, String pathToJar, int instanceId, String TopologyName) {
		try {
			URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			classname = classname.replace('/', '.');
			Class<?> componentClass = cl.loadClass(classname);
			Object componentInstance = componentClass.newInstance();

			ClassAndInstance classAndInstance = new ClassAndInstance();
			classAndInstance.setM_cClass(componentClass);
			classAndInstance.setM_oClassObject(componentInstance);
			classAndInstance.setM_nInstanceId(instanceId);
			
			//Is the next line needed?
			String instanceName = classname + String.valueOf(instanceId);
			m_hComponentInstances.put(instanceName, classAndInstance);
			
			String pathToZnodeInstance = new String("/Topologies/"+TopologyName+":"+classname+":"+instanceName);
			m_oZooKeeper.create(pathToZnodeInstance,m_sNodeIP,createNodeCallback);
			m_oZooKeeper.getData(pathToZnodeInstance, ComponentDataChangeWatcher, ComponentDataChangeCallback, null);
			//ZooKeeper zk = m_oZooKeepeer.createZKInstance(m_sZooKeeperConnectionIP, this);
			//DataMonitor dm = new DataMonitor(zk, pathToZnodeInstance, null, this);
			

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

		// Method createTopology = componentClass.getMethod("CreateTopology");
		// m_hTopologyList.put(JobName, (List<TopologyComponent>)
		// createTopology.invoke(topologyObject));

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
		m_oOutputTupleQ.WriteData(tuple);
	}

	// AddTask(String compName, String jobname, int instanceId)
	// ReceiveProcessedTuple(Tuple, pointerToTaskMan)
	// StartJob(String jobname) //This is because we cannot start it in AddTask
	// - wait to add all the tasks)

	@SuppressWarnings("unchecked")
	private void RetrieveTopologyComponents(String pathToJar, String topologyName) // (Thrift)
	{
		// Get the topology from the jar.
		// Receive the parallelism level
		// Do Round Robin
		try {
			URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			topologyName = topologyName.replace('/', '.');
			Class<?> topologyClass = cl.loadClass(topologyName);
			Object topologyObject = topologyClass.newInstance();

			Method createTopology = topologyClass.getMethod("CreateTopology");
			m_hTopologyList.put(topologyName, (Topology) createTopology.invoke(topologyObject));

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
	ArrayList<Tuple> IdentifyNextComponents(Tuple tuple) {
		
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		// get the source component
		String sSrc = tuple.m_sSrcCompName;
		// get the next comp list
		Topology topo = m_hTopologyList.get(tuple.m_sJobname);
		ArrayList<String> nextComp = topo.GetNextComponents(sSrc);
		if(nextComp.size() > 0)
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
				
			} else {
				inst = (int) (topo.Get(firtComp).nextTupleIndex() % 
						topo.Get(firtComp).getParallelismLevel());
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
					inst = (int) (topo.Get(nextCompName).nextTupleIndex() % 
							topo.Get(nextCompName).getParallelismLevel());
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
		if(m_hClusterInfo.containsKey(key))
		{
			sIP = m_hClusterInfo.get(key);
		} else
		{
			m_oLogger.Error("Unable to find the IP containing the instance " + key + ". Tuple "
						+ "will not proceed to next components" );
		} 
		return sIP;
	}

	// this call is made by the output disruptor handler
	public void SendToNextComponent(Tuple tuple) {

		ArrayList<Tuple> tuples = IdentifyNextComponents(tuple);

		m_oMutexOutputTuple.lock();
		for (int i = 0; i < tuples.size(); ++i) {
			String sIP = GetNextNode(tuples.get(i));
			if(sIP == null) continue;
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

}
