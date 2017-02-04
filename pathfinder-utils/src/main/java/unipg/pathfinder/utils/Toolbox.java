/**
 * 
 */
package unipg.pathfinder.utils;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class Toolbox {
	
	public static Iterable<PathfinderVertexID> getBranchEdgesForVertex(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex){
		HashSet<PathfinderVertexID> edgesToUse = new HashSet<PathfinderVertexID>();
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(current.getValue().isBranch())
				edgesToUse.add(current.getTargetVertexId().copy());
		}
		return edgesToUse;
	}

}
