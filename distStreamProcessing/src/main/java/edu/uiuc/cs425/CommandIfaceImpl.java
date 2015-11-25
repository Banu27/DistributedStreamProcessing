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
		
		public void ReceiveJob(String JobName, ByteBuffer data, String TopologyName, String Filename) throws TException {
			//m_oNodeManager.ReceiveJob(JobName, data, TopologyName, Filename);
		}
		
		public void CreateInstance(String classname, String pathToJar, int instanceId, String topologyName) {

			m_oNodeManager.CreateInstance(classname, pathToJar, instanceId, topologyName);
		}


		public void TransferTupleToNode(int nTuples, List<ByteBuffer> tuples) throws TException {
			// TODO Auto-generated method stub
			
		}
}
