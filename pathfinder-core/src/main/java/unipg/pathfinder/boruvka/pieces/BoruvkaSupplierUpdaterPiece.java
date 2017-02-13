/**
 * 
 */
package unipg.pathfinder.boruvka.pieces;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.function.Supplier;
import org.apache.hadoop.io.IntWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.suppliers.Suppliers.BoruvkaSupplier;

/**
 * @author spark
 *
 */
public class BoruvkaSupplierUpdaterPiece extends Piece<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage, Object> {

	BoruvkaSupplier bs;
	
	/**
	 * 
	 */
	public BoruvkaSupplierUpdaterPiece() {
		bs = new BoruvkaSupplier();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.piece.DefaultParentPiece#getVertexSender(org.apache.giraph.block_app.framework.api.BlockWorkerSendApi, java.lang.Object)
	 */
	@Override
	public VertexSender<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> getVertexSender(
			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi,
			Object executionStage) {
		return (vertex) -> {
			bs.updateFragments(((IntWritable)workerApi.getAggregatedValue(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator)).get());	
		};
	}
	
	public Supplier<Boolean> getSupplier(){
		return bs;
	}
	
}
