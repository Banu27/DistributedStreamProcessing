package edu.uiuc.cs425;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class GenSentenceFileSpout extends BComponent implements ISpout {
	
	private String sFolderPath;
	private BufferedReader bufferedReader;
	
	public GenSentenceFileSpout()
	{
		sFolderPath = "/home/ajayaku2/mp4/dataset/";
		bufferedReader = null;
	}

	public void nextTuple() {
		try {
			if(bufferedReader == null) 
			{
				
					File file = new File(sFolderPath + Integer.toString(getInstance()) + ".txt");
					FileReader fileReader = new FileReader(file);
					bufferedReader = new BufferedReader(fileReader);
			}
		
			Commons.sleep(100);
			String line;
			if ((line = bufferedReader.readLine()) != null) {
				if(line != null && line.length() >= 3)
				{
					Tuple tuple = new Tuple();
					tuple.AddElement("sentence", line);
					emit(tuple);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
	}
	
}
