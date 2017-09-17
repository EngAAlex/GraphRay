# GraphRay

GraphRay is a distributed algorithm to compute the Pathfinder network of a graph G with fixed parameters r = infinite and q = n-1. It is implemented on Giraph and requires Maven to compile. It is described in [our paper](https://goo.gl/HG3DHf).

```
@inproceedings{arleo17graphray,
    title={{GraphRay: Distributed Pathfinder Network Scaling}},
    author={Arleo, Alessio and Kwon, Oh-Hyun and Ma, Kwan-Liu},
    booktitle={IEEE Symposium on Large Data Analysis and Visualization},
    year={2017},  
    note={To appear}
}
```

# Usage

This is an example of script to use to launch the Giraph job:

```
yarn jar /path/to/graphray-core-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner \
	com.graphray.GraphRaySetup \
	 -mc com.graphray.masters.GraphRayMasterCompute \
	 -eif com.graphray.common.io.GraphRayEdgeInputFormat \
	 -eip /path/to/input/file/on/hdfs \
	 -eof com.graphray.common.io.GraphRayEdgeOutputFormat \
	 -op /path/to/output/folder/on/hdfs \
	 -ca giraph.SplitMasterWorker=false \
     -w {no of workers}
```

# Input Format

GraphRay input and output format is pretty simple. Each line represents a single edge with this structure:

\[source ID] \[target ID] [weight]

Bear in mind that GraphRay accepts undirected networks. 

# Options

| Option | Meaning | Type | Default |
| ------ | ------ | ------ | ------ |
| graphray.enableLogging | Enables verbose logging on syslog output. Debug use only | boolean | false |

# License

