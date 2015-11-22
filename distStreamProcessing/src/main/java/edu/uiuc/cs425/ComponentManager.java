package edu.uiuc.cs425;

import java.util.Date;
import java.util.List;

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
		
	
	public void ReceiveNewJob(String jobname, byte[] jarFile)  //(Thrift)
	{
		//Get the topology from the jar. 
		//Receive the parallelism level
		//Do Round Robin
		
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
