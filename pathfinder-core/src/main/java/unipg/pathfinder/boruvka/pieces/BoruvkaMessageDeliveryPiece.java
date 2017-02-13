/**
 * 
 */
package unipg.pathfinder.boruvka.pieces;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.mst.blocks.MSTBlockWithApiHandle;

/**
 * @author spark
 *
 */
public class BoruvkaMessageDeliveryPiece extends MSTBlockWithApiHandle{
//	
	BlockApiHandle bah;
//	
//	/**
//	 * 
//	 */
//	public BoruvkaMessageDeliveryPiece(BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi) {
//		this.workerSendApi = workerSendApi;
//	}
	
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
					getBlockApiHandle().getWorkerSendApi().sendMessage(vertexValue.getLoeDestination(), new ControlledGHSMessage(vertex.getId(), current.getFragmentID().copy(), current.getStatus()));
					vertex.getEdgeValue(vertexValue.getLoeDestination()).setAsBranchEdge(); //edge is marked as branch edge 
				}
				else if(!vertexValue.isRoot()){
					getBlockApiHandle().getWorkerSendApi().sendMessage(vertexValue.getFragmentIdentity(), current.copy());
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
