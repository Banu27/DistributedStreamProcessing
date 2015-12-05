package edu.uiuc.cs425;

import java.util.Date;
import java.util.HashMap;


public class WordCounterBolt extends BComponent implements IBolt {
	
	private HashMap<String, Long>  m_hCounter;
	private Date date;
	
	public WordCounterBolt()
	{
		m_hCounter = new HashMap<String, Long>();
		date = new Date();
	}
	
	public void execute(Tuple tuple) {
		// get the word
		String word = tuple.GetStringValue("word");
		//check if there is a key in the hash map
		if(m_hCounter.containsKey(word))
		{
			long count = m_hCounter.get(word).longValue();
			count++;
			m_hCounter.put(word, count);
			
		} else
		{
			m_hCounter.put(word, (long)1);
		}
		
		System.out.println("Added word - " + word +" count-" +  Long.toString(m_hCounter.get(word)));
		//add the word and increment the count
		
		//print the message
	}

}
