package edu.uiuc.cs425;

import java.util.Random;
import java.util.Date;

public class ExclamationSpout extends BComponent implements ISpout {
	
	private Random _rand;
	private Date   date;
	private long wordCounter;
	
	public ExclamationSpout()
	{
		wordCounter = 0;
		date = new Date();
		_rand = new Random(date.getTime());
	}

	public void nextTuple() {
		Commons.sleep(100);
		final String[] words = new String[] { "nathan", "mike", "jackson", "golda", "bertels" };
        final String word = words[_rand.nextInt(words.length)] + Long.toString(wordCounter++);
        Tuple tuple = new Tuple();
        tuple.AddElement("word",word);
        emit(tuple);
        System.out.println(Long.toString(date.getTime()) + " emit word:" + word );
	}

}
