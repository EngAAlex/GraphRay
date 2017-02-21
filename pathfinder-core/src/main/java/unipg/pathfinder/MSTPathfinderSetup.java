/**
 * 
 */
package unipg.pathfinder;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class MSTPathfinderSetup extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage> {

	/* (non-Javadoc)
	 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
			Iterable<ControlledGHSMessage> messages) throws IOException {
		super.compute(vertex, messages);
		vertex.getValue().setFragmentIdentity(vertex.getId().copy());
		log.info(" setting fragment " + vertex.getValue().getFragmentIdentity());
		if(vertex.getNumEdges() == 0){
			vertex.getValue().loesDepleted();
			vertex.voteToHalt();
		}
	}
	
}
