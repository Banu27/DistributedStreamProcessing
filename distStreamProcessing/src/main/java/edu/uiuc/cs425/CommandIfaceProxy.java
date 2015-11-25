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

		public void CreateInstance(String classname, String pathToJar) throws TException{
			// TODO Auto-generated method stub
			m_oClient.CreateInstance(classname, pathToJar);
		}
		
		public void ReceiveJob(String JobName, ByteBuffer data, String TopologyName, String Filename) throws TException{
			m_oClient.ReceiveJob(JobName, data, TopologyName, Filename);
		}

		public void TransferTupleToNode(int nTuples, List<ByteBuffer> tuples) throws TException {
			// TODO Auto-generated method stub
			
		}

		
			
		

				
}
