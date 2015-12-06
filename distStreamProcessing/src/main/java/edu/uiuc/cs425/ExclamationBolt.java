package edu.uiuc.cs425;

import java.util.Date;

public class ExclamationBolt extends BComponent implements IBolt {
	
	private Date   date;
	
	public ExclamationBolt()
	{
		date = new Date();
	}

	public void execute(Tuple tuple) {
		String value = tuple.GetStringValue("word");
		String newValue = value + "!!!";
		Tuple outTuple = new Tuple();
		outTuple.AddElement("word", newValue);
		emit(outTuple);
		System.out.println(Long.toString(date.getTime()) + " emit word:" + newValue );
	}

}
