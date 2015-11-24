package edu.uiuc.cs425;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Set;

import org.apache.thrift.TException;

public class Client {
		
		private CommandIfaceProxy 		m_oMasterProxy;
		private Logger			  		m_oLogger;
		private boolean           		m_bInit;
		private String					m_sMasterIP; //Config??
		private int						m_nMasterPort;	
		private String					m_sFilesSourceDir; //Config?
		
		Client(Logger logger)
		{
			m_oLogger 		= logger;
		}
		
		private int UpdateProxies() 
		{
		
			//What is this??
			
			//if(m_bInit) {
				//m_oMasterProxy.Close();
			//}
			int counter = 0;
			
			
			// continuous pinging for introducer to connect
			m_oLogger.Info("Adding Jar(): Updating introducer proxy");
			while(Commons.FAILURE == m_oMasterProxy.Initialize(m_sMasterIP, m_nMasterPort, m_oLogger))
			{
				if( counter++ > 10) 
				{
					m_oLogger.Error("Failed to initialize master proxy. Exiting after 10 tries");
					return Commons.FAILURE;
				}
				
				m_oLogger.Warning(new String("Failed to initialize master proxy. Trying in 1 secs"));
				System.out.println("Failed to initialize master proxy. Trying in 1 secs");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
					return Commons.FAILURE;
				}
				
			}
			
			//WHAT IS THIS?
			//m_bInit = true;
			return Commons.SUCCESS;
		}
		
		public int AddFile(String localPath, String FileName, String JobName, String TopologyName)
		{
			localPath = m_sFilesSourceDir + localPath;
			m_oLogger.Info("Adding Jar: Updating proxy");
			if(UpdateProxies() == Commons.FAILURE)
			{
				System.out.println("Unable to connect to Master");
				return Commons.FAILURE;
			}
			
			m_oLogger.Info("AddJar(): Finished Updating proxy");
			
			// make call to the node
			m_oLogger.Info("Addfile(): Creating a string buf of the file");
			
			ByteBuffer payload = null;
			FileInputStream fIn;
			FileChannel fChan;
			int fSize;
	        try {
				fIn = new FileInputStream(localPath);
			
			    fChan = fIn.getChannel();
				fSize = (int) fChan.size();
				payload = ByteBuffer.allocate((int) fSize);
				fChan.read(payload);
				payload.rewind();
				fChan.close(); 
				fIn.close();
	        } catch (FileNotFoundException e) {
	        	m_oLogger.Error(m_oLogger.StackTraceToString(e));
				return Commons.FAILURE;
			} catch (IOException e) {
				m_oLogger.Error(m_oLogger.StackTraceToString(e));
				return Commons.FAILURE;
			}
	        m_oLogger.Info("Addfile(): Transfer buffer to node");
			try {
				m_oMasterProxy.ReceiveJob(JobName, payload, TopologyName, FileName);
			} catch (TException e) {
				m_oLogger.Error(m_oLogger.StackTraceToString(e));
				return Commons.FAILURE;
			}
			m_oLogger.Info("Addfile(): Add file complete");
			return Commons.SUCCESS;
		}
		
		
}
