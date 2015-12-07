Similar to storm, crane has various data processing units called bolts/spouts. 
The spout receives input from the dataset and converts the data into tuples 
and sends it over to the next bolt/bolts. The bolts, spouts and the way they 
are linked constitutes the topology of the job. The client defines the topology 
required from the Application and the functions of the spout and bolts. These 
are submitted to crane in the form of a jar file. The client can also specify 
a parallelism level for each component. This refers to the number of instances 
of the component that will be available at any given time.
The crane instance receives the jar and creates a Topology object. 
The Master then creates bolt and spout instances which are assigned to 
the available workers. ZooKeeper is used to coordinate and store the 
information about the components present in every node and also the available
worker nodes. 


Usage

How to compile: 

$ cd distributedStreamProcessing $ mvn package 
This will create a fat jar in the target folder. 
Copy the jar to all the node. 
You can use the below command to copy the jar to all the nodes.
$ for NUM in seq 1 1 7; do scp crane.jar fa15-cs425-g01-0$NUM:~; done

To start execution :
Component Manager :
java -cp crane.jar edu.uiuc.cs425.ComponentManager conf.xml - master

NodeManager :
java -cp crane.jar edu.uiuc.cs425.NodeManager config.xml - nodemanager

Client :
java -cp crane.jar edu.uiuc.cs425.CraneClient config.xml 
    edu.uiuc.cs425.LegalPhraseFinderTopology crane.jar - sumiting jar
    
The config xml sample can be found in the DistributedStreamProcessing folder
