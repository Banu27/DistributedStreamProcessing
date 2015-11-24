package edu.uiuc.cs425;

public class TaskManager implements Runnable {

	private String 					m_sJobname;
	private String 					m_sComponentName;
	private int						m_nInstanceId;
	private IBolt 					m_oBolt;
	private ISpout 					m_oSpout;
	private NodeManager				m_oNM;
	private Thread					m_SpoutThread;
	private DisruptorBoltWrapper    m_oDisruptor;
	
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
		m_oDisruptor = new DisruptorBoltWrapper(2048, m_oBolt);
		m_oDisruptor.Init();
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
	
	public void SendTupleToNM(Tuple tuple)
	{
		m_oNM.ReceiveTupleFromTask(tuple);
	}
	
	public void AddTuple(Tuple tuple)
	{
		m_oDisruptor.WriteData(tuple);
	}

	public void run() {
		while(true)
		{
			m_oSpout.nextTuple();
		}
	}
	
	
	
}
