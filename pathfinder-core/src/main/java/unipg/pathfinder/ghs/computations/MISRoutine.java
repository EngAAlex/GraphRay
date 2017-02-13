/**
 * 
 */
package unipg.pathfinder.ghs.computations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;

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
public class MISRoutine {

	MasterCompute master;
	int counter = 0;

	public MISRoutine(MasterCompute master){
		this.master = master;	
	}

	public boolean compute(){
		if(counter == 0){
			master.setComputation(MISPing.class);
			counter++;
			return false;
		}else if(counter == 1){
			master.setComputation(MISReply.class);
			counter++;
			return false;
		}
		else if(counter == 2){
			master.setComputation(FragmentReconstructionPing.class);
			counter++;
			return false;
		}else if(counter == 3){
			master.setComputation(FragmentReconstructionReply.class);
			counter = 0;			
		}		
	return true;
}

public static class MISPing extends PathfinderComputation<ByteWritable, DoubleWritable>{

	/* (non-Javadoc)
	 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
			Iterable<ByteWritable> messages) throws IOException {
		PathfinderVertexType vertexValue = vertex.getValue();
		vertexValue.setRoot(false);							
		if(vertexValue.getDepth() == -1){ 
			vertexValue.setMISValue(Math.random());
			sendMessageToAllEdges(vertex, new DoubleWritable(vertexValue.getMISValue()));
		}else if(vertexValue.getDepth() == 1)
			vertexValue.setRoot(true);
	}		
}

public static class MISReply extends PathfinderComputation<DoubleWritable, DoubleWritable>{

	/* (non-Javadoc)
	 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
			Iterable<DoubleWritable> messages) throws IOException {
		PathfinderVertexType vertexValue = vertex.getValue();
		int vertexDepth = vertexValue.getDepth();
		if(vertexDepth != -1)
			return;							
		boolean foundSmaller = false;
		double myValue = vertexValue.getMISValue();
		Iterator<DoubleWritable> it = messages.iterator();			
		while(it.hasNext()){
			if(it.next().get() < myValue){
				foundSmaller = true;
				break;
			}
		}
		vertexValue.setRoot(!foundSmaller);
		vertexValue.resetDepth();
	}	
}

public static class FragmentReconstructionPing extends PathfinderComputation<DoubleWritable, PathfinderVertexID>{

	/* (non-Javadoc)
	 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
			Iterable<DoubleWritable> messages) throws IOException {
		PathfinderVertexType vertexValue = vertex.getValue();	
		Iterator<PathfinderVertexID> targets = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH).iterator();
		if(vertexValue.isRoot()){
			PathfinderVertexID fragmentIdentity = vertex.getId();
			vertexValue.setFragmentIdentity(fragmentIdentity);
			sendMessageToMultipleEdges(targets, fragmentIdentity.copy());
		}else
			return;			
	}	
}

public class FragmentReconstructionReply extends PathfinderComputation<PathfinderVertexID, ControlledGHSMessage>{

	/* (non-Javadoc)
	 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
			Iterable<PathfinderVertexID> messages) throws IOException {
		PathfinderVertexType vertexValue = vertex.getValue();
		if(!vertexValue.isRoot()){
			Iterator<PathfinderVertexID> msgs = messages.iterator();
			vertexValue.setFragmentIdentity(msgs.next().copy());
		}
	}

}

}
