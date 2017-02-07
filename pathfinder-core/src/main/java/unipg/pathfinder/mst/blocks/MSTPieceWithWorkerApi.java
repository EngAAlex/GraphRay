/**
 * 
 */
package unipg.pathfinder.mst.blocks;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.Piece;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public abstract class MSTPieceWithWorkerApi extends Piece<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage, Object> {

	protected BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi;

	public MSTPieceWithWorkerApi(BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi){
		this.workerSendApi = workerSendApi;
	}
	
}
