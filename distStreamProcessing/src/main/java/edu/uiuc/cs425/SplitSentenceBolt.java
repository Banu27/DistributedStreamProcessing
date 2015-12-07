package edu.uiuc.cs425;

public class SplitSentenceBolt extends BComponent implements IBolt {

	public void execute(Tuple tuple) {
		// get the sentence and split it
		String sentence = tuple.GetStringValue("sentence");
		String[] tokens = sentence.split(" ");
		// emit each word
		for( String str: tokens)
		{
			if(str != null && str.length() >=3)
			{
				Tuple outTuple = new Tuple();
				outTuple.AddElement("word", str);
				emit(outTuple);
			}
		}	
	}
}
