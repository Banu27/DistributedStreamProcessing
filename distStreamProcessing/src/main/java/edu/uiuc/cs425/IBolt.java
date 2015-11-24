package edu.uiuc.cs425;

public interface IBolt {

	// consume input tuple and emit output tuple
	public void execute(Tuple tuple);
	
}
