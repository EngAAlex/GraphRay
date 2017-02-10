/**
 * 
 */
package unipg.pathfinder.mst.boruvka.pieces;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class BoruvkaMessageDeliveryPiece extends Piece<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage, Object>{
	
	BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi;
	
	/**
	 * 
	 */
	public BoruvkaMessageDeliveryPiece(BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi) {
		this.workerSendApi = workerSendApi;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.piece.AbstractPiece#getVertexReceiver(org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi, java.lang.Object)
	 */
	@Override
	public VertexReceiver<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> getVertexReceiver(
			BlockWorkerReceiveApi<PathfinderVertexID> workerApi, Object executionStage) {
		return (vertex, messages) -> {
			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			PathfinderVertexType vertexValue = vertex.getValue();
			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID currentMsgFragment = current.getFragmentID();
				if(current.getFragmentID().equals(vertexValue.getFragmentIdentity())){ //the message is to be handed over to the destination cluster
					workerSendApi.sendMessage(vertexValue.getLoeDestination(), new ControlledGHSMessage(vertex.getId(), current.getFragmentID().copy(), current.getStatus()));
					vertex.getEdgeValue(vertexValue.getLoeDestination()).setAsBranchEdge(); //edge is marked as branch edge 
				}
				else if(!vertexValue.isRoot()){
					workerSendApi.sendMessage(vertexValue.getFragmentIdentity(), current.copy());
					vertex.getEdgeValue(current.getSenderID()).setAsBranchEdge(); //edge is marked as branch edge
				}else{
					if(vertex.getEdgeValue(current.getSenderID()) != null) //message is coming from another fragment directly to the root
						vertex.getEdgeValue(current.getSenderID()).setAsBranchEdge();					
					if(currentMsgFragment.get() > vertexValue.getFragmentIdentity().get()){ //the vertex is the discarded root
						vertexValue.setFragmentIdentity(currentMsgFragment.copy());
						vertexValue.setInactive();
					}
				}
			}
		};		
	}


}
