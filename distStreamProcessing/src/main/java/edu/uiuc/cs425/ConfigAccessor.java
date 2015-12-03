package edu.uiuc.cs425;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.io.IOException;


/*
 <MembershipConfig>
   <Cluster ipMaster="10.16.8.85" cmdPort="9090" zooIP="" />
   <Failure interval="30000" />
   <Logger path="" />
   <Topologies jarPath="" transferInterval="3000/>
</MembershipConfig>
 */

public class ConfigAccessor {
	private String 		m_sMasterIP;
	private int 		m_nCmdPort;
	private String 	    m_sZooIp;
	private int 		m_nFailureInterval;
	private String      m_sLogPath;
	private String      m_sJarPath;
	private int 	    m_nTransferInterval; //tuple
	
	public ConfigAccessor()
	{
		
	}
	
	public int TupleTransferInterval()
	{
		return m_nTransferInterval;
	}
	
	public String ZookeeperIP()
	{
		return m_sZooIp;
	}
	
	public String   JarPath()
	{
		return m_sJarPath;
	}
	public String       LogPath()
	{
		return m_sLogPath;
	}
	
	public String MasterIP()
	{
		return m_sMasterIP;
	}
	
	public int 		CmdPort()
	{
		return m_nCmdPort;
	}
	
	public int 		FailureInterval()
	{
		return m_nFailureInterval;
	}
	
	
	
	
	public int Initialize(String sXMLFilePath)
	{
		File fXmlFile = new File(sXMLFilePath);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Commons.FAILURE;
		}
		Document doc;
		try {
			doc = dBuilder.parse(fXmlFile);
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Commons.FAILURE;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Commons.FAILURE;
		}
				
		Node root = doc.getDocumentElement();
		NodeList nList = root.getChildNodes();

		for (int temp = 0; temp < nList.getLength(); temp++) {
			Node nNode = nList.item(temp);
			if (nNode.getNodeName() == "Cluster") {

				Element eElement = (Element) nNode;
				m_sMasterIP  = eElement.getAttribute("ipMaster");
				m_nCmdPort = Integer.parseInt(eElement.getAttribute("cmdPort"));
				m_sZooIp = eElement.getAttribute("zooIP");

			}  else if(nNode.getNodeName() == "Failure")
			{
				Element eElement = (Element) nNode;
				m_nFailureInterval  = Integer.parseInt(eElement.getAttribute("interval"));
			}
			else if(nNode.getNodeName() == "Logger")
			{
				Element eElement = (Element) nNode;
				m_sLogPath  = eElement.getAttribute("path");
			}
			else if(nNode.getNodeName() == "Topologies")
			{
				Element eElement = (Element) nNode;
				m_sJarPath  = eElement.getAttribute("jarPath");
				m_nTransferInterval = Integer.parseInt(eElement.getAttribute("transferInterval"));
			}
			
		}
		return Commons.SUCCESS;
	}
	

}

