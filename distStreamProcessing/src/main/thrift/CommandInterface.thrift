namespace java edu.uiuc.cs425

typedef i32 int

service CommandInterface{
  	void ReceiveJob(1:string JobName, 2:binary Jarfile, 3:string TopologyName, 4:string filename); #Component Manager to Nodes
	void CreateInstance(1:string classname, 2:string pathToJar, 3:int instanceId, 4:string topologyname);
	
	# this is called from NM to NM to transfer tuples
	void TransferTupleToNode(1:i32 nTuples, list<binary> tuples);
	
	bool isAlive();
}
