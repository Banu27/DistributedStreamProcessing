package edu.uiuc.cs425;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.thrift.TException;

public class CraneClient {

	public static void main( String[] args )
    {
		// takes three inputs
		// 0. xml
		// 1. topology name
		// 2. jar path
		
		if( args.length !=3 )
		{
			System.out.println("Usage: java -cp ~/crane.jar edu.uiuc.cs425.CraneClient <xml_path> <topology_name> <jar_path>");
			System.exit(Commons.FAILURE);
		}
		
		// get master ip from xml
		final ConfigAccessor m_oConfig = new ConfigAccessor();
		
		// instantiate logger and config accessor
		if( Commons.FAILURE == m_oConfig.Initialize(args[0]))
		{
			System.out.println("Failed to Initialize XML");
			System.exit(Commons.FAILURE);
		}
		
		final Logger m_oLogger = new Logger();
		if( Commons.FAILURE == m_oLogger.Initialize("crane_client.log"))
		{
			System.out.println("Failed to Initialize logger object");
			System.exit(Commons.FAILURE);
		}
		
		
		String masterIp = m_oConfig.MasterIP();
		// connect to master
		CommandIfaceProxy masterProxy = new CommandIfaceProxy();
		if( Commons.FAILURE == masterProxy.Initialize(masterIp, m_oConfig.CmdPort(), m_oLogger))
		{
			System.out.println("Unable to connect to master");
			System.exit(Commons.FAILURE);
		}
		// send the reqeust to master
		String file = args[2];
		FileInputStream fIn;
	    FileChannel fChan;
	    long fSize;
	    ByteBuffer mBuf = null;

	    try {
	      fIn = new FileInputStream(file);
	      fChan = fIn.getChannel();
	      fSize = fChan.size();
	      mBuf = ByteBuffer.allocate((int) fSize);
	      fChan.read(mBuf);
	      mBuf.rewind();
	      fChan.close(); 
	      fIn.close(); 
	    } catch (IOException exc) {
	      System.out.println(exc);
	      System.exit(Commons.FAILURE);
	    }
	    
		try {
			masterProxy.ReceiveJobFromClient(args[1], mBuf);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(Commons.FAILURE);
		}
		//exit
    }
}
