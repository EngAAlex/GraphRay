/**
 * 
 */
package unipg.pathdiner.mst.ghs.pieces;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.function.vertex.ConsumerWithVertex;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.mst.blocks.MSTPieceWithWorkerApi;

/**
 * @author spark
 *
 */
public class ConnectionPiece extends MSTPieceWithWorkerApi {

	/**
	 * @param workerSendApi
	 */
	public ConnectionPiece(
			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi) {
		super(workerSendApi);
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
