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
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ComponentManager {

		
	private	List<String>		m_lWorkersList;
	private int					m_nNextAssignableWorker;
	
	//Methods

	public void Initialize()
	{
		m_nNextAssignableWorker = 0;
	}
	
	
	public void AdmitNewWorker(String IP) //(Thrift)
	{
		m_lWorkersList.add(new String(IP + String.valueOf(GetMyLocalTime())));
	}
		
	
	public void ReceiveNewJob(String jobname, String pathToJar)  //(Thrift)
	{
		//Get the topology from the jar. 
		//Receive the parallelism level
		//Do Round Robin
		
		JarFile jarFile;
		try {
			jarFile = new JarFile(pathToJar);
			Enumeration<JarEntry> e = jarFile.entries();

			URL[] urls = { new URL("jar:file:" + pathToJar+"!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			    while (e.hasMoreElements()) {
			        JarEntry je = (JarEntry) e.nextElement();
			        if(je.isDirectory() || !je.getName().endsWith(".class")){
			            continue;
			        }
			    // -6 because of .class
			    String className = je.getName().substring(0,je.getName().length()-6);
			    className = className.replace('/', '.');
			    try {
					Class<?> c = cl.loadClass(className);
					try {
						Object object1 = c.newInstance();
						Method method = c.getMethod("getNum");
						System.out.println("Invoked method name: " + method.getName());
						int i = (Integer) method.invoke(object1);
						System.out.println(String.valueOf(i));

					} catch (InstantiationException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (IllegalAccessException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (IllegalArgumentException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (InvocationTargetException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (NoSuchMethodException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (SecurityException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				} catch (ClassNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			    }
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		
		int parallelismLevel = 0; //Get from the jar
		
		//For each component/task
		for(int i=0; i<parallelismLevel; i++)
		{
			//Add task at next available node
			
			//Updating this in this way so that there is continuation from one job to next
			//and one component to next
			m_nNextAssignableWorker = (m_nNextAssignableWorker + 1) % m_lWorkersList.size();
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
	
}
