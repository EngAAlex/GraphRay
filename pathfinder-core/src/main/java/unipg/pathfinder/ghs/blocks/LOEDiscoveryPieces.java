/**
 * 
 */
package unipg.pathfinder.ghs.blocks;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Stack;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.mst.blocks.MSTBlockWithApiHandle;
import unipg.pathfinder.utils.Toolbox;

public class LOEDiscoveryPieces{
	
	protected static Logger log = Logger.getLogger("LoeDiscoveryPieces");

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.piece.DefaultParentPiece#getVertexSender(org.apache.giraph.block_app.framework.api.BlockWorkerSendApi, java.lang.Object)
	 */
	public static SupplierFromVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> getLOEDiscoverySupplier() {
		return (vertex) -> {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.hasLOEsDepleted())
				return null;

			PathfinderVertexID selectedNeighbor = null;
			Stack<PathfinderVertexID> loes = null;

			if(vertexValue.getLOE() == Double.MAX_VALUE){
				loes = Toolbox.getLOEsForVertex(vertex, PathfinderEdgeType.UNASSIGNED);

				if(loes == null){
					vertexValue.loesDepleted();
					return null;
				}

				selectedNeighbor = loes.pop();			
				vertexValue.setLoeDestination(selectedNeighbor);

				for(PathfinderVertexID pfid : loes)
					vertex.getEdgeValue(pfid).setAsPathfinderCandidate();
			}else
				selectedNeighbor = vertexValue.getLoeDestination(); //SELECTED NEIGHBORS SHOULD BE SAVED AS WELL IN THE VERTEX TYPE

			return new ControlledGHSMessage(vertex.getId(), vertexValue.getFragmentIdentity(), vertexValue.getDepth(), ControlledGHSMessage.TEST_MESSAGE);
		};

	}

	public static SupplierFromVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterator<PathfinderVertexID>> getLOEDiscoveryTargetSelectionSupplier(){
		return (vertex) -> {
			HashSet<PathfinderVertexID> toReturn = new HashSet<PathfinderVertexID>();
			PathfinderVertexType vertexValue = vertex.getValue();
			toReturn.add(vertexValue.getLoeDestination());
			return toReturn.iterator();
		};
	}

	public static ConsumerWithVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterable<ControlledGHSMessage>> getLoeDiscoveryConsumerWithVertex(
			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi){
		return (vertex, messages) -> {
			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexType vertexValue = vertex.getValue();
			PathfinderVertexID myId = vertexId;
			PathfinderVertexID myFragment = vertexValue.getFragmentIdentity();
			Iterator<ControlledGHSMessage> msgIterator = messages.iterator();
			while(msgIterator.hasNext()){
				ControlledGHSMessage currentMessage = msgIterator.next();
				PathfinderVertexID currentSenderID = currentMessage.getSenderID();
				PathfinderVertexID currentFragment = currentMessage.getFragmentID();
				short currentMessageCode = currentMessage.getStatus();
				//					int msgDepth = currentMessage.getDepth();
				if(currentMessageCode == ControlledGHSMessage.TEST_MESSAGE){ 
					if(!myFragment.equals(currentFragment)){ //connection accepted
						workerApi.sendMessage(currentSenderID, 
								new ControlledGHSMessage(myId, ControlledGHSMessage.ACCEPT_MESSAGE));
					}else{ //connection refused
						workerApi.sendMessage(currentSenderID, 
								new ControlledGHSMessage(myId, ControlledGHSMessage.REFUSE_MESSAGE));
						if(vertexValue.getLoeDestination() == currentSenderID)
							vertexValue.resetLOE();
						if(!vertex.getEdgeValue(currentSenderID).isPathfinderCandidate())
							workerApi.removeEdgesRequest(vertexId, currentSenderID);							
					}
				}
			}
		};
	}
	
}



