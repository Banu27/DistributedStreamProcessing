package edu.uiuc.cs425;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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

public class NodeManager implements Runnable {

	private HashMap<String, List<String>> m_hTaskListPerJob;
	private HashMap<String, List<String>> m_hClusterInfo;
	private HashMap<String, List<TopologyComponent>> m_hTopologyList;
	private HashMap<String, ClassAndInstance> m_hComponentInstances;

	private String m_sJarFilesDir;
	private String m_sMyIp;

	// list of all the worker currently in the cluster
	private ArrayList<String> m_lWorkerIPs;

	// data structure to store the tuples to be transferred to other nodes
	// a thread will go through them every interval and transfer them to other
	// nodes. We want to tansfer as a batch
	private HashMap<String, Queue<Tuple>> m_OutputTuples;
	private int m_nTransferInterval;
	private Lock m_oMutexOutputTuple;

	// these are the node level input and output queues. The
	// handlers and producers for the disruptor are implemented
	// within the class. Different init methods needs to be called
	// for each of the disruptor type.
	private DisruptorWrapper m_oInputTupleQ;
	private DisruptorWrapper m_oOutputTupleQ;

	// hash map for ip to commandinterface proxy. Idea is to cache the
	// proxies and not to recreate them every time
	private HashMap<String, CommandIfaceProxy> m_hIPtoProxy;

	private Logger m_oLogger;
	private ConfigAccessor m_oConfig;

	public NodeManager() {
		m_hTaskListPerJob = new HashMap<String, List<String>>();
		m_hClusterInfo = new HashMap<String, List<String>>();
		m_hTopologyList = new HashMap<String, List<TopologyComponent>>();
		m_hComponentInstances = new HashMap<String, ClassAndInstance>();
		m_oMutexOutputTuple = new ReentrantLock(true);
		try {
			m_sMyIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {

		}
		m_lWorkerIPs = new ArrayList<String>();
	}

	public void Initialize(Logger logger, ConfigAccessor config) {
		m_oInputTupleQ.InitNodeInput(this);
		m_oOutputTupleQ.InitNodeOutput(this);
		m_oLogger = logger;
		m_oConfig = config;
	}

	public int WriteFileIntoDir(ByteBuffer file, String filename) {
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

	public void ReceiveJob(String JobName, ByteBuffer jar, String TopologyName, String Filename) 
	{
		WriteFileIntoDir(jar, Filename);
		String pathToJar = m_sJarFilesDir + '/' + Filename;
		RetrieveTopologyComponents(JobName, pathToJar, TopologyName);
		// Send jars to all the workers.
	}

	public void CreateInstance(String classname, String pathToJar) {
		try {
			URL[] urls = { new URL("jar:file:" + pathToJar + "!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			classname = classname.replace('/', '.');
			Class<?> componentClass = cl.loadClass(classname);
			Object componentInstance = componentClass.newInstance();

			ClassAndInstance classAndInstance = new ClassAndInstance();
			classAndInstance.setM_cClass(componentClass);
			classAndInstance.setM_oClassObject(componentInstance);
			m_hComponentInstances.put(classname, classAndInstance);

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
		}

		// Method createTopology = componentClass.getMethod("CreateTopology");
		// m_hTopologyList.put(JobName, (List<TopologyComponent>)
		// createTopology.invoke(topologyObject));

	}

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
	private void RetrieveTopologyComponents(String JobName, String pathToJar, String TopologyName) // (Thrift)
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
			m_hTopologyList.put(JobName, (List<TopologyComponent>) createTopology.invoke(topologyObject));

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
		// look at the topology and if there are n components
		// the tuple needs to go to then create a ArrayList of size
		// n and all the input tuple and (n-1) other new tuples to
		// the array.
		ArrayList<Tuple> arrayList = null;
		return arrayList;
	}

	public String GetNextNode(Tuple tuple) {
		// look at the job, component and instance info in
		// the tuple and select the node
		String retStr = null;
		return retStr;
	}

	// this call is made by the output disruptor handler
	public void SendToNextComponent(Tuple tuple) {

		ArrayList<Tuple> tuples = IdentifyNextComponents(tuple);

		m_oMutexOutputTuple.lock();
		for (int i = 0; i < tuples.size(); ++i) {
			String sIP = GetNextNode(tuple);
			Queue<Tuple> queue = m_OutputTuples.get(sIP);
			if (queue == null)
				m_OutputTuples.put(sIP, new LinkedList<Tuple>());
			queue = m_OutputTuples.get(sIP);
			queue.add(tuple);
		}
		m_oMutexOutputTuple.unlock();
	}

	// this call is made by the input disruptor handler
	public void SendTupleToTask(Tuple tuple) {
		//look at the destination fields in the tuple and decide which 
		//task should get it
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
			Set<String> sIPs = m_OutputTuples.keySet();
			// iterate through the keys
			for (String sIP : sIPs) {
				// send list of tuples to the other node
				Queue<Tuple> queue = m_OutputTuples.get(sIP);
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
