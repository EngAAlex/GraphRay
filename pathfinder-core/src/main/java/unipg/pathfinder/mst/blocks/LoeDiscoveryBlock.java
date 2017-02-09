/**
 * 
 */
package unipg.pathfinder.mst.blocks;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathdiner.mst.ghs.pieces.ReportDeliveryPiece;
import unipg.pathdiner.mst.ghs.pieces.ReportGeneratorPiece;
import unipg.pathfinder.mst.ghs.blocks.LOEDiscoveryPieces;

/**
 * @author spark
 *
 */
public class LoeDiscoveryBlock extends MSTPieceWithWorkerApi {

	/**
	 * @param workerSendApi
	 */
	public LoeDiscoveryBlock(
			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi) {
		super(workerSendApi);
	}

	public Block getLOEDiscoveryBlock(){
		
		return new SequenceBlock(
			Pieces.<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage>sendMessage("LOEDiscovery", ControlledGHSMessage.class, 
						LOEDiscoveryPieces.getLOEDiscoverySupplier(),
						LOEDiscoveryPieces.getLOEDiscoveryTargetSelectionSupplier(),
						LOEDiscoveryPieces.getLoeDiscoveryConsumerWithVertex(workerSendApi)),
			new ReportGeneratorPiece(workerSendApi),
			new ReportDeliveryPiece(workerSendApi)
		);
	}
	
}
