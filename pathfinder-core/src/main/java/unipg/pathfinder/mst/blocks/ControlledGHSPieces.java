/**
 * 
 */
package unipg.pathfinder.mst.blocks;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.hadoop.io.BooleanWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;

public class ControlledGHSPieces{
	
	/**
	 * @author spark
	 *
	 */
	public static class LOEDiscovery extends Piece<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage, Object> {

		/* (non-Javadoc)
		 * @see org.apache.giraph.block_app.framework.piece.DefaultParentPiece#getVertexSender(org.apache.giraph.block_app.framework.api.BlockWorkerSendApi, java.lang.Object)
		 */
		@Override
		public VertexSender<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> getVertexSender(
				BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi,
				Object executionStage) {
			return null;
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.block_app.framework.piece.DefaultParentPiece#getVertexSender(org.apache.giraph.block_app.framework.api.BlockWorkerSendApi, java.lang.Object)
		 */
		public SupplierFromVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> getVertexMessageProvider(
				BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi,
				Object executionStage) {
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

		public SupplierFromVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterable<PathfinderVertexID>> getTargetSelection(){
			return (vertex) -> {
				HashSet<PathfinderVertexID> toReturn = new HashSet<PathfinderVertexID>();
				PathfinderVertexType value = vertex.getValue();
				toReturn.add(new PathfinderVertexID(value.getLoeDestination(), vertex.getId().getLayer()));
				return toReturn;
			};
		} 

		public ConsumerWithVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterable<ControlledGHSMessage>> getTestMessagesDeliveryVertexConsumer(
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

		public ConsumerWithVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterable<ControlledGHSMessage>> getTestMessagePingVertexConsumer(
				BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi){
			return(vertex, messages) -> {
				PathfinderVertexID vertexId = vertex.getId();
				PathfinderVertexType vertexValue = vertex.getValue();
				if(vertexValue.hasLoesDepleted() && !vertexValue.isRoot()){					
					workerApi.sendMessage(new PathfinderVertexID(vertexValue.getFragmentIdentity(), vertexId.getLayer()), new ControlledGHSMessage(vertex.getId().get(), ControlledGHSMessage.LOEs_DEPLETED));
					return;
				}
				Iterator<ControlledGHSMessage> msgs = messages.iterator();
				while(msgs.hasNext()){
					ControlledGHSMessage currentMessage = msgs.next();
					long currentFragment = currentMessage.getFragmentID();
					short currentMessageCode = currentMessage.getStatus();
					switch(currentMessageCode){
					case ControlledGHSMessage.REFUSE_MESSAGE:
						PathfinderVertexID remoteID = new PathfinderVertexID(currentFragment, vertexId.getLayer());
						if(!vertex.getEdgeValue(remoteID).isPathfinderCandidate())
							workerApi.removeEdgesRequest(vertexId, remoteID); vertexValue.resetLOE(); break;						
					case ControlledGHSMessage.ACCEPT_MESSAGE:
						if(!vertexValue.isRoot())
							workerApi.sendMessage(new PathfinderVertexID(vertexValue.getFragmentIdentity(), vertexId.getLayer()), new ControlledGHSMessage(vertexId.get(), vertexValue.getLOE(), ControlledGHSMessage.REPORT_MESSAGE));
						break;
					}
				}
			};		
		}

		public static class LOEChoiceAndConnection extends Piece<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage, Object>{
			
			public ConsumerWithVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterable<ControlledGHSMessage>> getLOEChoiceVertexConsumer(
					BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi){
				return (vertex, messages) -> {
					PathfinderVertexType vertexValue = vertex.getValue();
					if(!vertexValue.isRoot()) //only roots will react now, and I'm sure only them will have messages incoming
						return;
					PathfinderVertexID vertexId = vertex.getId();
					Iterator<ControlledGHSMessage> msgs = messages.iterator();
					long minLOEDestination = vertexValue.getLoeDestination();
					double minLOE = vertexValue.getLOE();
					while(msgs.hasNext()){
						ControlledGHSMessage currentMessage = msgs.next();
						short currentStatus = currentMessage.getStatus();
						if(currentStatus == ControlledGHSMessage.LOEs_DEPLETED)
							continue;
						long currentSenderID = currentMessage.getSenderID();
						double currentValue = currentMessage.get();
						if(currentValue < minLOE){
							minLOE = currentValue;
							minLOEDestination = currentSenderID;
						}
					}
					if(minLOE != Double.MAX_VALUE){
						workerApi.sendMessage(new PathfinderVertexID(minLOEDestination, vertexId.getLayer()), new ControlledGHSMessage(vertexId.get(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.CONNECT_MESSAGE));
						workerApi.aggregate(MSTPathfinderMasterCompute.procedureCompletedAggregator, new BooleanWritable(false));
					}
				};
			}
			
			public ConsumerWithVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterable<ControlledGHSMessage>> getConnectReplyVertexConsumer(
					BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi){
				return (vertex, messages) -> {
					PathfinderVertexType vertexValue = vertex.getValue();
					vertexValue.resetLOE();
					Iterator<ControlledGHSMessage> msgs = messages.iterator();					
					if(!msgs.hasNext())
						return;
					PathfinderVertexID vertexId = vertex.getId();
					long myLOEDestination = vertexValue.getLoeDestination();
					long myfragmentIdentity = vertexValue.getFragmentIdentity();
					while(msgs.hasNext()){
						ControlledGHSMessage current = msgs.next();
						long msgFragmentIdentity = current.getFragmentID();
						long msgSender = current.getSenderID();
						if(msgFragmentIdentity == myfragmentIdentity){ //CONNECT MESSAGE CROSSED THE FRAGMENT BORDER
							workerApi.sendMessage(new PathfinderVertexID(myLOEDestination, vertexId.getLayer()), new ControlledGHSMessage(vertexId.get(), myfragmentIdentity, ControlledGHSMessage.CONNECT_MESSAGE));
						}else if(myLOEDestination == msgSender){ //CONNECT MESSAGE CROSSED THE FRAGMENT BORDER
							vertex.getEdgeValue(new PathfinderVertexID(msgSender, vertexId.getLayer())).setAsBranchEdge();//FRAGMENTS AGREE ON THE COMMON EDGE
							vertexValue.addBranch();
							vertexValue.resetLOE();
						}
					}
				};
			}
		}
		
	}
}



