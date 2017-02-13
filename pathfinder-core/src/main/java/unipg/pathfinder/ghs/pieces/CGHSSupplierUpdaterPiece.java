/**
 * 
 */
package unipg.pathfinder.ghs.pieces;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.function.Supplier;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.suppliers.Suppliers.BoruvkaSupplier;
import unipg.pathfinder.suppliers.Suppliers.CGHSSupplier;

/**
 * @author spark
 *
 */
public class CGHSSupplierUpdaterPiece extends Piece<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage, Object> {

	CGHSSupplier cghss;
	
	/**
	 * 
	 */
	public CGHSSupplierUpdaterPiece() {
		cghss = new CGHSSupplier();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.piece.AbstractPiece#masterCompute(org.apache.giraph.block_app.framework.api.BlockMasterApi, java.lang.Object)
	 */
	@Override
	public void masterCompute(BlockMasterApi masterApi, Object executionStage) {
		Logger log = Logger.getLogger(this.getClass());
		log.info("machecazzo " + cghss.get());
		try{
			cghss.update(((BooleanWritable)masterApi.getAggregatedValue(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator)).get());
		}catch(NullPointerException npe){
			log.info("Porco il cazzo " + masterApi == null + " " + ((BooleanWritable)masterApi.getAggregatedValue(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator) == null));
		}
	}
//	
//	/* (non-Javadoc)
//	 * @see org.apache.giraph.block_app.framework.piece.DefaultParentPiece#getVertexSender(org.apache.giraph.block_app.framework.api.BlockWorkerSendApi, java.lang.Object)
//	 */
//	@Override
//	public VertexSender<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> getVertexSender(
//			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi,
//			Object executionStage) {
//		return (vertex) -> {
//
//		};
//	}
//	
	public Supplier<Boolean> getSupplier(){
		return cghss;
	}
	
}

	