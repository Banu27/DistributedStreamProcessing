package edu.uiuc.cs425;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.SerializationUtils;

public class Tuple implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<String,Object> elements;
	
	// the fields will be filled by the generating spout/bolt
	public String 					m_sJobname;
	public String 					m_sSrcCompName;
	public int						m_nSrcInstId;
	
	// these values will be filled in by the NM and used by the
	// receiving NM to locate the processing component
	public String 					m_sDestCompName;
	public int						m_nDestInstId;
	
	// IMPORTANT: Fill in the method if more variables are added to the class
	public void Copy(Tuple tuple)
	{
		tuple.elements 			= elements;
		tuple.m_sJobname 		= m_sJobname;
		tuple.m_sSrcCompName 	= m_sSrcCompName;
		tuple.m_nSrcInstId 		= m_nSrcInstId;
		tuple.m_sDestCompName	= m_sDestCompName;
		tuple.m_nDestInstId		= m_nDestInstId;
	}
	
	
	
	public Tuple()
	{
		elements 		= new HashMap<String, Object>();		
	}
	
	public void SetSrcFieldInfo(String jobName, String comp, int instId)
	{
		m_sJobname 		= jobName;
		m_sSrcCompName 	= comp;
		m_nSrcInstId 	= instId;
	}
	
	public void SetDestinationFields(String comp, int inst)
	{
		m_sDestCompName 	= comp;
		m_nDestInstId 		= inst;
	}
	
	public void AddElement(String key, int value)
	{
		elements.put(key, Integer.toString(value));
	}
	
	public void AddElement(String key, String value)
	{
		elements.put(key, value);
	}
	
	public int GetIntValue(String key)
	{
		return (Integer)elements.get(key);
	}
	
	public String GetStringValue(String key)
	{
		return elements.get(key).toString();
	}
	
	public Object GetValue(String key)
	{
		return elements.get(key);
	}
	
	public byte[] Serialize()
	{
		return SerializationUtils.serialize(this);
	}
	
	
	
}
