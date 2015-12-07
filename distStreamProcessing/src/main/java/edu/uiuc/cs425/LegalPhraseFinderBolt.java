package edu.uiuc.cs425;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class LegalPhraseFinderBolt extends BComponent implements IBolt {

	private HashMap<String, Long>		m_hCounter;
	private String						m_sOutputFolderPath;
	private FileWriter 					m_oWriter;
	
	public LegalPhraseFinderBolt() 
	{
		m_hCounter = new HashMap<String, Long>();
		
		String filePath = "/home/ajayaku2/mp4/dataset/database";
		m_sOutputFolderPath = null;
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
		    String line;
		    while ((line = br.readLine()) != null) {
		    	m_hCounter.put(line, (long) 0);
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void execute(Tuple tuple) {
		// get the word
		String word = tuple.GetStringValue("word").toLowerCase();
		//check if there is a key in the hash map
		if(m_hCounter.containsKey(word))
		{
			long count = m_hCounter.get(word).longValue();
			count++;
			m_hCounter.put(word, count);
			//Print here
			if(m_sOutputFolderPath == null)
			{
				m_sOutputFolderPath = "/home/ajayaku2/mp4/dataset/output";
				File file = new File(m_sOutputFolderPath + Integer.toString(getInstance()));
				try {
					file.createNewFile();
					m_oWriter = new FileWriter(file); 
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				m_oWriter.write(word + " : " + String.valueOf(count) + "\n");
				m_oWriter.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}	
		//	System.out.println("Added word - " + word +" count-" +  Long.toString(m_hCounter.get(word)));
	}

}
