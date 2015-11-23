package edu.uiuc.cs425;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ComponentManager implements Runnable{

		
	private	List<String>									m_lWorkersList;
	private int												m_nNextAssignableWorker;
	private HashMap<String, List<TopologyComponent>> 		m_hTopologyList;
	
	//Methods

	public void Initialize()
	{
		m_nNextAssignableWorker = 0;
	}
	
	
	public void AdmitNewWorker(String IP) //(Thrift)
	{
		m_lWorkersList.add(new String(IP + String.valueOf(GetMyLocalTime())));
	}
	
	public void ReceiveNewJob(String JobName, File file, String TopologyName)
	{
		//THis function does file reading 
		//saves file in the disk and gets the localpath
		String pathToJar = null;
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
				String classname = component.getM_sComponentName();
				Class<?> componentClass = cl.loadClass(classname);
				
				for(int i=0; i<component.getM_nParallelismLevel(); i++)
				{
					//Call the start task at the worker
					
				}
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
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
