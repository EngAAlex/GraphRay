/**
 * 
 */
package com.graphray.utils;

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
//import org.apache.log4j.Logger;

import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;
import com.graphray.common.writables.SetWritable;

/**
 * @author spark
 *
 */
public class Toolbox {
	
//	protected static Logger log = Logger.getLogger(Toolbox.class);

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
	
	public static Collection<PathfinderVertexID> getUnassignedEdgesByWeight(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, double value){
		HashSet<PathfinderVertexID> edgesToUse = null;
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(current.getValue().unassigned() && current.getValue().get() == value){
				if(edgesToUse == null)
					edgesToUse = new HashSet<PathfinderVertexID>();
				edgesToUse.add(current.getTargetVertexId().copy());
			}
		}
		return edgesToUse;
	}
	
	public static Stack<PathfinderVertexID> getLOEsForVertex(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex){
		return getLOEsForVertex(vertex, PathfinderEdgeType.UNASSIGNED);
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
		vertex.getValue().updateLOE(min);
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
	
	@SuppressWarnings("unchecked")
	public static void armFragmentPathfinderCandidates(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID edgesToActivate) throws IOException{
		Iterator<PathfinderVertexID> edges = vertex.getValue().peekSetOutOfStack(edgesToActivate).iterator();
		while(edges.hasNext()){
			PathfinderVertexID current = edges.next();
			if(vertex.getEdgeValue(current).getStatus() == PathfinderEdgeType.PATHFINDER_CANDIDATE){
				updateEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER, current);
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
				updateRemoteEdgeWithStatus(computation, vertex.getId(), remoteID, current.getValue(), PathfinderEdgeType.PATHFINDER);
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

	public static void disarmPathfinderCandidates(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex){
		Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			if(current.getValue().getStatus() == PathfinderEdgeType.PATHFINDER_CANDIDATE){
				updateEdgeValueWithStatus(vertex, PathfinderEdgeType.UNASSIGNED, current.getTargetVertexId());
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
		if(!vertex.getEdgeValue(currentSenderID).isBranch())
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

	public static void setMultipleEdgesAsCandidates(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, Iterable<PathfinderVertexID> recipients){
		updateMultipleEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER_CANDIDATE, recipients);
	}

	public static void setMultipleEdgesAsInterfragment(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, Iterable<PathfinderVertexID> recipients){
		updateMultipleEdgeValueWithStatus(vertex, PathfinderEdgeType.INTERFRAGMENT_EDGE, recipients);
	}

	public static void updateEdgeValueWithStatus(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, short newStatus, PathfinderVertexID recipient){
		if(vertex.getEdgeValue(recipient).getStatus() == newStatus)
			return;
		PathfinderEdgeType pet = vertex.getEdgeValue(recipient);
//		Logger.getLogger(Toolbox.class).info("Setting edge from " + vertex.getId() + " to " + recipient + " as " + PathfinderEdgeType.CODE_STRINGS[newStatus]);
		vertex.setEdgeValue(recipient, new PathfinderEdgeType(pet.get(), newStatus));		
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
	
	public static void updateMultipleEdgeValueWithStatus(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, short newStatus, Iterable<PathfinderVertexID> recipients, Collection<PathfinderVertexID> toExclude){
		if(recipients != null)
			for(PathfinderVertexID recipient : recipients){
				if(!toExclude.contains(recipient))
					updateEdgeValueWithStatus(vertex, newStatus, recipient);
				//			PathfinderEdgeType pet = vertex.getEdgeValue(recipient).copy();
				//			pet.setStatus(newStatus);
				//			vertex.setEdgeValue(recipient, pet);		
			}
	}

	@SuppressWarnings("rawtypes")
	public static void updateRemoteEdgeWithStatus(AbstractComputation computation, PathfinderVertexID sourceID, PathfinderVertexID remoteID, PathfinderEdgeType existingEdge, short status) throws IOException{
		computation.removeEdgesRequest(remoteID, sourceID);
//		PathfinderEdgeType newEdge = existingEdge.copy();
//		newEdge.setStatus(status);
//		Logger.getLogger(Toolbox.class).info("Setting edge from " + remoteID + " to " + sourceID + " as " + PathfinderEdgeType.CODE_STRINGS[status]);
		computation.addEdgeRequest(remoteID,
				EdgeFactory.create(sourceID, new PathfinderEdgeType(existingEdge.get(), status)));		
	}

	public static void connectWithDummies(AbstractComputation computation, Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID fragmentToConnect) throws IOException{
		if(vertex.getEdgeValue(fragmentToConnect) == null){
			computation.addEdgeRequest(vertex.getId(), EdgeFactory.create(fragmentToConnect, new PathfinderEdgeType(PathfinderEdgeType.DUMMY))); //new pair is created
			computation.addEdgeRequest(fragmentToConnect, EdgeFactory.create(vertex.getId(), new PathfinderEdgeType(PathfinderEdgeType.DUMMY)));
		}
	}

	@SuppressWarnings("rawtypes")
	public static void removeExistingDummies(AbstractComputation computation, Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID fragmentToConnect) throws IOException{
		if(vertex.getEdgeValue(fragmentToConnect) != null && vertex.getEdgeValue(fragmentToConnect).isDummy()){
			computation.removeEdgesRequest(vertex.getId(), fragmentToConnect);
			computation.removeEdgesRequest(fragmentToConnect, vertex.getId());
		}
	}

	/**
	 * @param branchConnector
	 * @param id
	 * @param selectedNeighbor
	 * @throws IOException 
	 */
	public static void setRemoteEdgeAsBranch(AbstractComputation computation, PathfinderVertexID id, PathfinderEdgeType existingEdge,
			PathfinderVertexID selectedNeighbor) throws IOException {
		updateRemoteEdgeWithStatus(computation, id, selectedNeighbor, existingEdge, PathfinderEdgeType.BRANCH);
	}
	
	public static void removeSetFromActiveFragmentStack(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID fragmentToRemove, short oldCondition) {
		SetWritable<PathfinderVertexID> toRemove = vertex.getValue().popSetOutOfStack(fragmentToRemove);
		if(toRemove != null)
			Toolbox.updateMultipleEdgeValueWithStatus(vertex, oldCondition, toRemove);
//		
	}

	/**
	 * 
	 */
	public static void removeSetFromActiveFragmentStack(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID fragmentToRemove) {
		SetWritable<PathfinderVertexID> toRemove = vertex.getValue().popSetOutOfStack(fragmentToRemove);
		if(toRemove != null)
			Toolbox.updateMultipleEdgeValueWithStatus(vertex, PathfinderEdgeType.UNASSIGNED, toRemove);	
	}
	
	/**
	 * @param vertex
	 * @param recipientsForFragment
	 */
	public static void setMultipleEdgesAsPathfinder(
			Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
			SetWritable<PathfinderVertexID> recipients) {
		updateMultipleEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER, recipients);		
	}
	
	/**
	 * @param vertex
	 * @param recipientsForFragment
	 */
	public static void setMultipleEdgesAsPathfinder(
			Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
			SetWritable<PathfinderVertexID> recipients, Collection<PathfinderVertexID> toExclude) {
		updateMultipleEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER, recipients, toExclude);		
	}
}
