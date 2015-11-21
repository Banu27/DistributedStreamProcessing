package edu.uiuc.cs425;

import java.util.Date;
import java.util.List;

public class ComponentManager {

		
	private	List<String>		WorkersList;
	
	//Methods

	public void AdmitNewWorker(String IP) //(Thrift)
	{
		WorkersList.add(new String(IP + String.valueOf(GetMyLocalTime())));
	}
		
	
	public void ReceiveNewJob(String jobname, byte[] jarFile)  //(Thrift)
	{
		//Get the topology from the jar. 
		//Receive the parallelism level
		//Do Round Robin
		
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
