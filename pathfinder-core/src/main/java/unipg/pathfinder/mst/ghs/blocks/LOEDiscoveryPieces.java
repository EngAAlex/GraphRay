/**
 * 
 */
package unipg.pathfinder.mst.ghs.blocks;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

public class LOEDiscoveryPieces{

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.piece.DefaultParentPiece#getVertexSender(org.apache.giraph.block_app.framework.api.BlockWorkerSendApi, java.lang.Object)
	 */
	public static SupplierFromVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> getLOEDiscoverySupplier() {
		return (vertex) -> {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.hasLoesDepleted())
				return null;
			long selectedNeighbor = -1;

			if(vertexValue.getLOE() == Double.MAX_VALUE){
				Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
				double min = Double.MAX_VALUE;
				HashSet<PathfinderVertexID> pathfinderCandidates = new HashSet<PathfinderVertexID>();
				while(edges.hasNext()){
					Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
					boolean isBranch = current.getValue().isBranch();
					if(isBranch)
						continue;
					double currentEdgeValue = current.getValue().get();
					long currentNeighbor = current.getTargetVertexId().get();
					if(currentEdgeValue < min){
						min = currentEdgeValue;
						selectedNeighbor = currentNeighbor;
						pathfinderCandidates.clear();
					}else if(currentEdgeValue == min)
						pathfinderCandidates.add(current.getTargetVertexId());											

				}
				if(min == Double.MAX_VALUE){
					vertexValue.loesDepleted();
				}
				for(PathfinderVertexID pfid : pathfinderCandidates)
					vertex.getEdgeValue(pfid).setAsPathfinderCandidate();
				vertexValue.setLoeDestination(selectedNeighbor);
			}else
				selectedNeighbor = vertexValue.getLoeDestination(); //SELECTED NEIGHBORS SHOULD BE SAVED AS WELL IN THE VERTEX TYPE
			return new ControlledGHSMessage(vertex.getId().get(), vertexValue.getFragmentIdentity(), vertexValue.getDepth(), ControlledGHSMessage.TEST_MESSAGE);
		};		
	}

	public static SupplierFromVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterator<PathfinderVertexID>> getLOEDiscoveryTargetSelectionSupplier(){
		return (vertex) -> {
			HashSet<PathfinderVertexID> toReturn = new HashSet<PathfinderVertexID>();
			PathfinderVertexType value = vertex.getValue();
			toReturn.add(new PathfinderVertexID(value.getLoeDestination(), vertex.getId().getLayer()));
			return toReturn.iterator();
		};
	} 

	public static ConsumerWithVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterable<ControlledGHSMessage>> getLoeDiscoveryConsumerWithVertex(
			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi){
		return (vertex, messages) -> {
			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexType vertexValue = vertex.getValue();
			long myId = vertexId.get();
			long myFragment = vertexValue.getFragmentIdentity();
			Iterator<ControlledGHSMessage> msgIterator = messages.iterator();
			while(msgIterator.hasNext()){
				ControlledGHSMessage currentMessage = msgIterator.next();
				long currentSenderID = currentMessage.getSenderID();
				long currentFragment = currentMessage.getFragmentID();
				short currentMessageCode = currentMessage.getStatus();
				//					int msgDepth = currentMessage.getDepth();
				if(currentMessageCode == ControlledGHSMessage.TEST_MESSAGE){ 
					if(myFragment != currentFragment){ //connection accepted
						workerApi.sendMessage(new PathfinderVertexID(currentSenderID, vertexId.getLayer()), 
								new ControlledGHSMessage(myId, ControlledGHSMessage.ACCEPT_MESSAGE));
					}else{ //connection refused
						workerApi.sendMessage(new PathfinderVertexID(currentSenderID, vertexId.getLayer()), 
								new ControlledGHSMessage(myId, ControlledGHSMessage.REFUSE_MESSAGE));
						if(vertexValue.getLoeDestination() == currentSenderID)
							vertexValue.resetLOE();
						if(!vertex.getEdgeValue(new PathfinderVertexID(currentSenderID, vertexId.getLayer())).isPathfinderCandidate())
							workerApi.removeEdgesRequest(vertexId, new PathfinderVertexID(currentSenderID, vertexId.getLayer()));							
					}
				}
			}
		};
	}
}



