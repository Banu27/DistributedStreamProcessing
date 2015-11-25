package edu.uiuc.cs425;

public class TaskManager implements Runnable {

	public String 					m_sJobname;
	public String 					m_sComponentName;
	public int						m_nInstanceId;
	private IBolt 					m_oBolt;
	private ISpout 					m_oSpout;
	private NodeManager				m_oNM;
	private Thread					m_SpoutThread;
	private DisruptorWrapper    m_oDisruptor;
	
	public TaskManager()
	{
		m_oDisruptor = null;
	}
	
	public void Init(String jobName, String compName, int id, IBolt obj, NodeManager oNM)
	{
		m_sJobname 				= jobName;
		m_sComponentName		= compName;
		m_nInstanceId			= id;
		m_oBolt					= obj;
		m_oNM					= oNM;
		m_oDisruptor 			= new DisruptorWrapper(2048);
		m_oDisruptor.InitBolt(m_oBolt);
	}
	
	public void Init(String jobName, String compName, int id, ISpout obj, NodeManager oNM)
	{
		m_sJobname 				= jobName;
		m_sComponentName		= compName;
		m_nInstanceId			= id;
		m_oSpout				= obj;
		m_oNM					= oNM;
		m_SpoutThread 			= new Thread(this);
		m_SpoutThread.start();

	}
	
	// this call is made by spout/bolt when they emit tuples. the emit impl is in the BComponent class
	public void SendTupleToNM(Tuple tuple)
	{
		m_oNM.ReceiveProcessedTuple(tuple, this);
	}
	
	// this is called from NM. This adds the tuple to the disruptor. A correponding
	// event handler will be invoked by the disruptor frameowrk to handle this tuple
	public void AddTuple(Tuple tuple)
	{
		m_oDisruptor.WriteData(tuple);
	}

	
	// this method is run in case of spouts. 
	public void run() {
		while(true)
		{
			m_oSpout.nextTuple();
		}
	}
	
	
	
}
