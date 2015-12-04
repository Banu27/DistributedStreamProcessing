package edu.uiuc.cs425;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.uiuc.cs425.CommandInterface.Iface;

public class CommandIfaceProxy implements Iface{
		
		private CommandInterface.Client m_oClient;
		private TTransport transport;
		private Logger m_oLogger;
		
		public CommandIfaceProxy()
		{
			m_oClient = null;
		}
		
		public int Initialize(String sIP,int nPort,Logger oLogger)
		{
			m_oLogger	= oLogger;
			transport = new TFramedTransport(new TSocket(sIP, nPort));
		    try {
				transport.open();
			} catch (TTransportException e) {
				// TODO Auto-generated catch block
				m_oLogger.Error(m_oLogger.StackTraceToString(e));
				m_oLogger.Error(new String("Failed to initialize command proxy")); //IP????
				return Commons.FAILURE;
			}
		    m_oClient = new CommandInterface.Client(new TBinaryProtocol(transport));
		    m_oLogger.Info(new String("Created command Proxy"));
			return Commons.SUCCESS;
		}

		

		public void TransferTupleToNode(int nTuples, List<ByteBuffer> tuples) throws TException {
			m_oClient.TransferTupleToNode(nTuples, tuples);	
		}

		public boolean isAlive() {
			try {
				return m_oClient.isAlive();
			} catch (TException e) {
				return false;
			}
		}

		public void ReceiveJobFromClient(String TopologyName, ByteBuffer Jarfile) throws TException {
			m_oClient.ReceiveJobFromClient(TopologyName, Jarfile);
			
		}


		public void CreateTask(String compName, String topologyname, int instanceId) throws TException {
			m_oClient.CreateTask(compName, topologyname, instanceId);
			
		}

		public void AddWorker(String sIP) throws TException {
			m_oClient.AddWorker(sIP);
			
		}

		public ByteBuffer GetJarFromMaster(String sTopologyName) throws TException {
			// TODO Auto-generated method stub
			return m_oClient.GetJarFromMaster(sTopologyName);
		}

		

		
			
		

				
}
