/**
 * 
 */
package unipg.pathfinder.blocks_framework.mst.blocks;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.blocks_framework.ghs.blocks.LOEDiscoveryPieces;
import unipg.pathfinder.blocks_framework.ghs.pieces.ReportDeliveryPiece;
import unipg.pathfinder.blocks_framework.ghs.pieces.ReportGeneratorPiece;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;

/**
 * @author spark
 *
 */
public class LoeDiscoveryBlock extends MSTBlockWithApiHandle {
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.piece.AbstractPiece#registerAggregators(org.apache.giraph.block_app.framework.api.BlockMasterApi)
	 */
	@SuppressWarnings("deprecation")
	@Override
	public void registerAggregators(BlockMasterApi masterApi) throws InstantiationException, IllegalAccessException {
		masterApi.registerAggregator(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, BooleanAndAggregator.class);
	}
	
	/**
	 * @param workerSendApi
	 */
	public Block getBlock() {
		return new SequenceBlock(
				Pieces.<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage>sendMessage("LOEDiscovery", ControlledGHSMessage.class, 
							LOEDiscoveryPieces.getLOEDiscoverySupplier(),
							LOEDiscoveryPieces.getLOEDiscoveryTargetSelectionSupplier(),
							LOEDiscoveryPieces.getLoeDiscoveryConsumerWithVertex(getBlockApiHandle().getWorkerSendApi())),
				new ReportGeneratorPiece(),
				new ReportDeliveryPiece()
				);
	}
	
}
