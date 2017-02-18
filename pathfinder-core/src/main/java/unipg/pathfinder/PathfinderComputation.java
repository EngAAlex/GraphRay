/**
 * 
 */
package unipg.pathfinder;

import java.io.IOException;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class PathfinderComputation<I extends Writable, M extends Writable> extends AbstractComputation<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, I, M> {

	protected static Logger log = Logger.getLogger(PathfinderComputation.class);
	
	
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, Iterable<I> messages)
			throws IOException {
		log.info("im " + vertex.getId());
	}


}
