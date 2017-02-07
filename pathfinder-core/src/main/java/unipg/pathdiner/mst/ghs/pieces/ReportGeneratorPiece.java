/**
 * 
 */
package unipg.pathdiner.mst.ghs.pieces;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.mst.blocks.MSTPieceWithWorkerApi;

/**
 * @author spark
 *
 */
public class ReportGeneratorPiece extends MSTPieceWithWorkerApi {

	/**
	 * @param workerSendApi
	 */
	public ReportGeneratorPiece(
			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi) {
		super(workerSendApi);
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.piece.AbstractPiece#getVertexReceiver(org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi, java.lang.Object)
	 */
	@Override
	public VertexReceiver<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> getVertexReceiver(
			BlockWorkerReceiveApi<PathfinderVertexID> workerApi, Object executionStage) {
		return(vertex, messages) -> {
			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.hasLoesDepleted() && !vertexValue.isRoot()){					
				workerSendApi.sendMessage(new PathfinderVertexID(vertexValue.getFragmentIdentity(), vertexId.getLayer()), new ControlledGHSMessage(vertex.getId().get(), ControlledGHSMessage.LOEs_DEPLETED));
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
						workerSendApi.removeEdgesRequest(vertexId, remoteID); vertexValue.resetLOE(); break;						
				case ControlledGHSMessage.ACCEPT_MESSAGE:
					if(!vertexValue.isRoot())
						workerSendApi.sendMessage(new PathfinderVertexID(vertexValue.getFragmentIdentity(), vertexId.getLayer()), new ControlledGHSMessage(vertexId.get(), vertexValue.getLOE(), ControlledGHSMessage.REPORT_MESSAGE));
					break;
				}
			}
		};
	}

}
