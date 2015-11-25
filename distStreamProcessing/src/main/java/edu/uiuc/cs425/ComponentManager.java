package edu.uiuc.cs425;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.thrift.TException;

public class ComponentManager implements Runnable{

		
	private	List<String>									m_lWorkersList;
	private int												m_nNextAssignableWorker;
	private HashMap<String, List<TopologyComponent>> 		m_hTopologyList;
	private Logger											m_oLogger;
	private int												m_nCommandServicePort;
	private String											m_sJarFilesDir;
	//Methods

	public void Initialize()
	{
		m_nNextAssignableWorker = 0;

		//This needs to be assigned	
		m_sJarFilesDir = null;
	}
	
	
	public void AdmitNewWorker(String IP) //(Thrift)
	{
		//m_lWorkersList.add(new String(IP + String.valueOf(GetMyLocalTime())));
		m_lWorkersList.add(IP);
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
	
	

	public void ReceiveNewJob(String JobName, ByteBuffer file, String TopologyName, String filename)
	{
		//Client has to give the file here after converting to bytebuffer
		WriteFileIntoDir(file, filename);
		String pathToJar = m_sJarFilesDir + '/' + filename;
		RetrieveTopologyComponents(JobName, pathToJar, TopologyName);
		//Send jars to all the workers.
		StartComponentsAtNodes(JobName, pathToJar);
	}
	
	
	void StartComponentsAtNodes(String Jobname, String pathToJar)
	{
		List<TopologyComponent> Components = m_hTopologyList.get(Jobname);
		
		try {
			URL[] urls = { new URL("jar:file:" + pathToJar+"!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);
			
			Iterator<TopologyComponent> iterator = Components.iterator();
			while(iterator.hasNext())
			{
				TopologyComponent component = iterator.next();
				String classname = component.getComponentName();
				Class<?> componentClass = cl.loadClass(classname);
				
				for(int i=0; i<component.getParallelismLevel(); i++)
				{
					//Call the start task at the worker
					CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
					if(Commons.SUCCESS == ProxyTemp.Initialize(m_lWorkersList.get(m_nNextAssignableWorker), m_nCommandServicePort, m_oLogger))
					{
						ProxyTemp.CreateInstance(classname, pathToJar);
					}
					
				}
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
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

			try {
				Method createTopology = topologyClass.getMethod("CreateTopology");
				m_hTopologyList.put(JobName, (List<TopologyComponent>) createTopology.invoke(topologyObject));
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
		}

	}

	public void ScheduleJob()	//Calls start job after creating calling AddTask
	{
		//Called from Receive new job
		//Assigns component instances to nodes
		//and initializes tasks
		//Calls start task
	}
	
	private long GetMyLocalTime()
	{
		return new Date().getTime();
	}


	public void run() {
		// TODO Auto-generated method stub
		
	}
	
}
