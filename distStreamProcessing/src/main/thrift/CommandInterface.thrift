namespace java edu.uiuc.cs425

typedef i32 int

service CommandInterface{
  	oneway void ReceiveJobFromClient(1:string TopologyName, 2:binary Jarfile, 3:string JarFileName); #client to comp manager
	oneway void CreateTask(1:string compName, 2:string topologyname , 3:int instanceId); #compmanager to nm
	
	# this is called from NM to NM to transfer tuples
	oneway void TransferTupleToNode(1:i32 nTuples, list<binary> tuples);
	
	bool isAlive(); 
	
	#nm to master
	oneway void AddWorker(1:string sIP); 
	binary GetJarFromMaster(1:string sTopologyName);
}
