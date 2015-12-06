package edu.uiuc.cs425;

public class BComponent {

	public TaskManager m_oTaskMgr; 
	
	public void emit(Tuple tuple)
	{
		m_oTaskMgr.SendTupleToNM(tuple);
	}
	
	public void SetTaskManager(TaskManager task)
	{
		m_oTaskMgr = task;
	}
	
	public int getInstance()
	{
		return m_oTaskMgr.GetInstance();
	}
}

