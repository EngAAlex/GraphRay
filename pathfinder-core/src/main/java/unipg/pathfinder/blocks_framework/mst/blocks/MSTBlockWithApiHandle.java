/**
 * 
 */
package unipg.pathfinder.blocks_framework.mst.blocks;

import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.block_app.framework.block.BlockWithApiHandle;
import org.apache.giraph.block_app.framework.piece.Piece;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class MSTBlockWithApiHandle extends Piece<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage, Object> implements BlockWithApiHandle {

	private BlockApiHandle bah;
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.block.BlockWithApiHandle#getBlockApiHandle()
	 */
	@Override
	public BlockApiHandle getBlockApiHandle() {
		if(bah == null)
			bah = new BlockApiHandle();
		return bah;
	}

}
