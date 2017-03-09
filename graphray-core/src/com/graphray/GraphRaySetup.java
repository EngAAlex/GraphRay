/**
 * 
 */
package com.graphray;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;

import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.messagetypes.ControlledGHSMessage;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class GraphRaySetup extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage> {

	/* (non-Javadoc)
	 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
			Iterable<ControlledGHSMessage> messages) throws IOException {
		super.compute(vertex, messages);
		vertex.getValue().setFragmentIdentity(vertex.getId().copy());
		if(isLogEnabled)
			log.info(" setting fragment " + vertex.getValue().getFragmentIdentity());
		if(vertex.getNumEdges() == 0){
			vertex.getValue().loesDepleted();
			vertex.voteToHalt();
		}
	}
	
}
