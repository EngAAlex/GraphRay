/**
 * 
 */
package unipg.pathfinder.utils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Stack;

import org.apache.commons.lang.ArrayUtils;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class Toolbox {

	protected static Logger log = Logger.getLogger(Toolbox.class);

	public static Collection<PathfinderVertexID> getSpecificEdgesForVertex(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, short ... condition){
		HashSet<PathfinderVertexID> edgesToUse = null;
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(ArrayUtils.indexOf(condition, current.getValue().getStatus(), 0) != -1){
				if(edgesToUse == null)
					edgesToUse = new HashSet<PathfinderVertexID>();
				edgesToUse.add(current.getTargetVertexId().copy());
			}
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

	public static PathfinderVertexID popPathfinderCandidate(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex){
		PathfinderVertexID pfid = null;
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(current.getValue().getStatus() == PathfinderEdgeType.PATHFINDER_CANDIDATE){
				pfid = current.getTargetVertexId().copy();
				break;
			}
		}
		return pfid;
	}



	@SuppressWarnings("unchecked")
	public static void armPathfinderCandidates(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex) throws IOException{
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(current.getValue().getStatus() == PathfinderEdgeType.PATHFINDER_CANDIDATE){
				PathfinderVertexID remoteID = current.getTargetVertexId().copy();
				updateEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER, remoteID);
			}
		}		
	}

	public static void armRemotePathfinderCandidates(AbstractComputation computation, Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex) throws IOException{
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(current.getValue().getStatus() == PathfinderEdgeType.PATHFINDER_CANDIDATE){
				PathfinderVertexID remoteID = current.getTargetVertexId().copy();
				updateEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER, remoteID);
				log.info("Arming remote pathfinder " + remoteID + " to " + vertex.getId());
				computation.removeEdgesRequest(remoteID, vertex.getId());
				computation.addEdgeRequest(remoteID,
						EdgeFactory.create(vertex.getId(), new PathfinderEdgeType(current.getValue().get(), PathfinderEdgeType.PATHFINDER)));
			}
		}		
	}

	public static void disarmPathfinderCandidates(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, short oldCondition){
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(current.getValue().getStatus() == PathfinderEdgeType.PATHFINDER_CANDIDATE){
				updateEdgeValueWithStatus(vertex, oldCondition, current.getTargetVertexId());
			}
		}
	}

	/**
	 * 
	 */
	public static void setEdgeAsInterFragmentEdge(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID currentSenderID) {
		updateEdgeValueWithStatus(vertex, PathfinderEdgeType.INTERFRAGMENT_EDGE, currentSenderID);	
	}

	/**
	 * 
	 */
	public static void setEdgeAsBranch(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID currentSenderID) {
		vertex.getValue().addBranch();
		updateEdgeValueWithStatus(vertex, PathfinderEdgeType.BRANCH, currentSenderID);	
	}		

	/**
	 * 
	 */
	public static void setEdgeAsPathfinderCandidate(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID currentSenderID) {
		updateEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER_CANDIDATE, currentSenderID);	
	}	
	/**
	 * 
	 */
	public static void consolidatePathfinder(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID currentSenderID) {
		updateEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER, currentSenderID);	
	}

	public static void updateEdgeValueWithStatus(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, short newStatus, PathfinderVertexID recipient){
		PathfinderEdgeType pet = vertex.getEdgeValue(recipient);
		//		pet.setStatus(newStatus);
		log.info("Setting edge from " + vertex.getId().get() + " to " + recipient.get() + " as " + PathfinderEdgeType.CODE_STRINGS[newStatus]);
		vertex.setEdgeValue(recipient, new PathfinderEdgeType(pet.get(), newStatus));		
	}

	public static void setMultipleEdgesAsCandidates(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, Iterable<PathfinderVertexID> recipients){
		updateMultipleEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER_CANDIDATE, recipients);
	}

	public static void setMultipleEdgesAsInterfragment(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, Iterable<PathfinderVertexID> recipients){
		updateMultipleEdgeValueWithStatus(vertex, PathfinderEdgeType.INTERFRAGMENT_EDGE, recipients);
	}

	public static void updateMultipleEdgeValueWithStatus(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, short newStatus, Iterable<PathfinderVertexID> recipients){
		if(recipients != null)
			for(PathfinderVertexID recipient : recipients){
				updateEdgeValueWithStatus(vertex, newStatus, recipient);
				//			PathfinderEdgeType pet = vertex.getEdgeValue(recipient).copy();
				//			pet.setStatus(newStatus);
				//			vertex.setEdgeValue(recipient, pet);		
			}
	}
}
