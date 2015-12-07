package edu.uiuc.cs425;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;

import edu.uiuc.cs425.CommandInterface.Iface;

public class CommandIfaceImpl implements Iface {

		private ComponentManager	m_oComponentManager;
		private NodeManager			m_oNodeManager;
		
		public int Initialize(ComponentManager oComponentManager, NodeManager  oNodeManager)
		{
			this.m_oComponentManager = oComponentManager;
			this.m_oNodeManager = oNodeManager;
			
			return Commons.SUCCESS;
		}
		
		
		

		public void TransferTupleToNode(int nTuples, List<ByteBuffer> tuples) throws TException {
			//System.out.println("Received " + Integer.toString(nTuples) + " tuples");
			m_oNodeManager.ReceiveTuplesFromOutside(tuples);	
		}

		public boolean isAlive() throws TException {
			return true;
		}



		public void ReceiveJobFromClient(String TopologyName, ByteBuffer Jarfile) throws TException {
			m_oComponentManager.ReceiveNewJob(Jarfile, TopologyName);	
		}



		public void CreateTask(String compName, String topologyname, String fullClassName, int instanceId) throws TException {
			m_oNodeManager.CreateTask(compName, instanceId, topologyname,fullClassName);
		}




		public void AddWorker(String sIP) throws TException {
			m_oComponentManager.AdmitNewWorker(sIP);
		}




		public ByteBuffer GetJarFromMaster(String sTopologyName) throws TException {
			return m_oComponentManager.GetJarFromMaster(sTopologyName);
		}




		public void StartTopology(String sTopoloogy) throws TException {
			m_oNodeManager.StartTopology(sTopoloogy);	
		}







		
}
