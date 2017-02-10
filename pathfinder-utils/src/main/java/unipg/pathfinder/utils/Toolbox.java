/**
 * 
 */
package unipg.pathfinder.utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Stack;

import org.apache.commons.lang.ArrayUtils;
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
	
	public static Iterable<PathfinderVertexID> getSpecificEdgesForVertex(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, short ... condition){
		HashSet<PathfinderVertexID> edgesToUse = new HashSet<PathfinderVertexID>();
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();			
			if(ArrayUtils.indexOf(condition, current.getValue().getStatus(), 0) != -1)
				edgesToUse.add(current.getTargetVertexId().copy());
		}
		return edgesToUse;
	}
	
	public static Stack<PathfinderVertexID> getLOEsForVertex(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, short condition){
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		Stack<PathfinderVertexID> loes = null;
		double min = Double.MAX_VALUE;
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(current.getValue().getStatus() != condition)
				continue;
			double currentEdgeValue = current.getValue().get();
			PathfinderVertexID currentNeighbor = current.getTargetVertexId();
			if(currentEdgeValue < min){
				min = currentEdgeValue;
				if(loes == null)
					loes = new Stack<PathfinderVertexID>();
				loes.clear();
				loes.push(currentNeighbor.copy());
			}else if(currentEdgeValue == min)
				loes.push(currentNeighbor.copy());			
		}
		return loes;
	}
	
	public static void armPathfinderCandidates(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex){
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(current.getValue().getStatus() == PathfinderEdgeType.PATHFINDER_CANDIDATE)
				current.getValue().consolidatePathfinder();
		}		
	}
	
	public static void disarmPathfinderCandidates(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex){
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(current.getValue().getStatus() == PathfinderEdgeType.PATHFINDER_CANDIDATE)
				current.getValue().revertToUnassigned();
		}
	}

}
