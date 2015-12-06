package edu.uiuc.cs425;

import java.util.Date;
import java.util.Random;

public class SentenceGenSpout extends BComponent implements ISpout {

	private Random _rand;
	
	public SentenceGenSpout()
	{
		_rand = new Random(new Date().getTime());
	}
	
	public void nextTuple() {
		Commons.sleep(10000);
		String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
		        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
		
		String sentence = sentences[_rand.nextInt(sentences.length)];
		System.out.println("Spout: sentence: " + sentence);
		Tuple tuple = new Tuple();
		tuple.AddElement("sentence", sentence);
		emit(tuple);
	}

}
