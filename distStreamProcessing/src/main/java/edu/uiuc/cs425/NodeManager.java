package edu.uiuc.cs425;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.List;

public class NodeManager {

	private HashMap<String, List<String>>				m_hTaskListPerJob;
	private HashMap<String, List<String>>				m_hClusterInfo;
	private HashMap<String, List<TopologyComponent>>	m_hTopologyList;
	private HashMap<String, ClassAndInstance>			m_hComponentInstances;
	private String 										m_sJarFilesDir;
	//ClusterInfo 		Key - Component
		//		Value - Next instance locations
	
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
	
	
	public void ReceiveJob(String JobName, ByteBuffer jar, String TopologyName, String Filename) //To get the topology and bolt code etc
	{
		WriteFileIntoDir(jar, Filename);
		String pathToJar = m_sJarFilesDir + '/' + Filename;
		RetrieveTopologyComponents(JobName, pathToJar, TopologyName);
		//Send jars to all the workers.
	}

	public void CreateInstance(String classname, String pathToJar)
	{
		try
		{
			URL[] urls = { new URL("jar:file:" + pathToJar+"!/") };
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
		
		//Method createTopology = componentClass.getMethod("CreateTopology");
		//m_hTopologyList.put(JobName, (List<TopologyComponent>) createTopology.invoke(topologyObject));

	}
	
	public void ReceiveProcessedTuple(Tuple tuple, TaskManager task)
	{
		//add to disruptor queue
	}
	
	//AddTask(String compName, String jobname, int instanceId)
	//ReceiveProcessedTuple(Tuple, pointerToTaskMan)
	//StartJob(String jobname) //This is because we cannot start it in AddTask - wait to add all the tasks)

	@SuppressWarnings("unchecked")
	private void RetrieveTopologyComponents(String JobName, String pathToJar, String TopologyName)  //(Thrift)
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

}
