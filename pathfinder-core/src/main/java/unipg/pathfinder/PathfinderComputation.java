/**
 * 
 */
package unipg.pathfinder;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.Writable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class PathfinderComputation extends BasicComputation<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Writable> {

	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> arg0,
			Iterable<Writable> arg1) throws IOException {
		//NO-OP?
	}


}
