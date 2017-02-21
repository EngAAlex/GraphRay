/**
 * 
 */
package unipg.pathfinder;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

public class DummyComputations {

	/**
	 * @author spark
	 *
	 */
	public static class DummyEdgesCleanupComputation extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage> {

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
			super.compute(vertex, messages);
			while(edges.hasNext()){
				Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
				log.info("Current Edge " + current.getTargetVertexId() + " " + PathfinderEdgeType.CODE_STRINGS[current.getValue().getStatus()]);
				if(!current.getValue().isBranch() && !current.getValue().isPathfinder()){
					log.info("Deleted");
					removeEdgesRequest(vertex.getId(), current.getTargetVertexId().copy());
				}
			}
		}
	}
	
	public static class NOOPComputation extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{
		
		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();			
			while(edges.hasNext()){
				Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
				log.info("Final sweep Edge " + current.getTargetVertexId() + " " + PathfinderEdgeType.CODE_STRINGS[current.getValue().getStatus()]);
			}
			vertex.voteToHalt();
		}
		
	}

}
