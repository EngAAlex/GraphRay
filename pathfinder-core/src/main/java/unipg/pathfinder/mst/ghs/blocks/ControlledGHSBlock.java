/**
 * 
 */
package unipg.pathfinder.mst.ghs.blocks;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.IfBlock;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathdiner.mst.ghs.pieces.ConnectionPiece;
import unipg.pathdiner.mst.ghs.pieces.ReportDeliveryPiece;
import unipg.pathdiner.mst.ghs.pieces.ReportGeneratorPiece;
import unipg.pathfinder.mst.blocks.LoeDiscoveryBlock;

/**
 * @author spark
 *
 */
public class ControlledGHSBlock implements Supplier<Boolean>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -9092253790081261598L;

	BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi;
	
	public static final String procedureCompletedAggregator = "AGG_COMPLETE_GHS";
		
	LoeDiscoveryBlock ldb;
	ReportGeneratorPiece rgp;
	ReportDeliveryPiece rdp;
	ConnectionPiece cp;
	
	NegatedControlledGHSBlockSupplier ncgs;
	
	public ControlledGHSBlock(BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi){
		this.workerSendApi = workerSendApi;
		ldb = new LoeDiscoveryBlock(workerSendApi);
		rgp = new ReportGeneratorPiece(workerSendApi);
		rdp = new ReportDeliveryPiece(workerSendApi);
		cp = new ConnectionPiece(workerSendApi);
		ncgs = new NegatedControlledGHSBlockSupplier(this);
	}

	public Block getBlock() {
		return new RepeatUntilBlock(0, 
				new SequenceBlock(
					ldb.getLOEDiscoveryBlock(),
					new IfBlock(
							ncgs, getLOEConnectionAndFragmentUpdateBlock())
					), 
				 this);
	}
	
	private Block getLOEConnectionAndFragmentUpdateBlock(){
		return new SequenceBlock(
				new ConnectionPiece(workerSendApi),
				LeafDiscoveryBlock.createBlock(),
				MISComputationBlock.createBlock()
		);
	}

	/* (non-Javadoc)
	 * @see com.google.common.base.Supplier#get()
	 */
	@Override
	public Boolean get() {
		return ((BooleanWritable)workerSendApi.getAggregatedValue(procedureCompletedAggregator)).get();
	}
	
	public static class NegatedControlledGHSBlockSupplier implements Supplier<Boolean> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 6200534517192651871L;
		ControlledGHSBlock supplier;
		
		/**
		 * 
		 */
		public NegatedControlledGHSBlockSupplier(ControlledGHSBlock supplier) {
			this.supplier = supplier;
		}
		
		/* (non-Javadoc)
		 * @see org.apache.giraph.function.Supplier#get()
		 */
		@Override
		public Boolean get() {
			return !supplier.get();
		}
		
		
		
	}

}
