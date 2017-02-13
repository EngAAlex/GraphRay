/**
 * 
 */
package unipg.pathfinder.ghs.computations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.ByteWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
import unipg.pathfinder.utils.Toolbox;

/**
 * @author spark
 *
 */
public class LeafDiscoveryRoutine {
	
	MasterCompute master;
	int counter = 0;
	
	public LeafDiscoveryRoutine(MasterCompute master){
		this.master = master;	
	}
	
	public boolean compute(){
		if(counter == 0 || counter == 2){
			master.setComputation(LeafDiscoveryPing.class);
			counter++;
			return false;
		}else if(counter == 1){
			master.setComputation(LeafDiscoveryReply.class);
			counter++;
			return false;
		}else if(counter == 3){
			counter = 0;
			master.setComputation(LeafDiscoveryReply.class);			
		}
		return true;
	}
	

	public static class LeafDiscoveryPing extends PathfinderComputation<ControlledGHSMessage, ByteWritable> {
		
		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.isIsolated())
				return;
			
			Iterator<PathfinderVertexID> destinations = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH).iterator();

			if(vertexValue.getDepth() == -1){
				if(vertexValue.isLeaf()){
					vertexValue.setDepth((byte) 0);
					sendMessageToMultipleEdges(destinations, new ByteWritable((byte) 0));
				}else 
					sendMessageToMultipleEdges(destinations, new ByteWritable((byte) 1));
			}else
				return;
		}		
	}

	public static class LeafDiscoveryReply extends PathfinderComputation<ByteWritable, ByteWritable>{
		
		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ByteWritable> messages) throws IOException {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.getDepth() != -1)
				return;
			Iterator<ByteWritable> msgs = messages.iterator();
			switch(msgs.next().get()){
			case (byte) 0: 
				vertexValue.setDepth((byte) 0);
				break;
			case (byte) 1: 
				vertexValue.setDepth((byte) 1);
				break;
			}
		}	
	}
	
}
