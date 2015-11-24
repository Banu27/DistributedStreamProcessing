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
	
	
	public Tuple()
	{
		elements = new HashMap<String, Object>();
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
