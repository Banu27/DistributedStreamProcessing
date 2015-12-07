package edu.uiuc.cs425;

import java.io.BufferedReader;

public class GenSentenceFileSpout extends BComponent implements ISpout {
	
	private String sFolderPath;
	private BufferedReader bufferedReader;
	
	private GenSentenceFileSpout()
	{
		sFolderPath = "/home/ajayaku2/dataset/";
		bufferedReader = null;
	}

	public void nextTuple() {
		if(bufferedReader == null) 
		{
			
		}
	}
	
}
