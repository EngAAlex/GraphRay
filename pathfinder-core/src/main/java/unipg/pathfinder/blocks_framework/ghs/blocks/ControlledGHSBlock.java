/**
 * 
 */
package unipg.pathfinder.blocks_framework.ghs.blocks;

import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.IfBlock;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.function.Supplier;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.log4j.Logger;

import unipg.pathfinder.blocks_framework.ghs.pieces.CGHSSupplierUpdaterPiece;
import unipg.pathfinder.blocks_framework.ghs.pieces.ConnectionPiece;
import unipg.pathfinder.blocks_framework.ghs.pieces.ReportDeliveryPiece;
import unipg.pathfinder.blocks_framework.ghs.pieces.ReportGeneratorPiece;
import unipg.pathfinder.blocks_framework.mst.blocks.LoeDiscoveryBlock;

/**
 * @author spark
 *
 */
public class ControlledGHSBlock implements Supplier<Boolean>{
		
	/**
	 * 
	 */
	private static final long serialVersionUID = -9092253790081261598L;

//	BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi;
	
	public static final String procedureCompletedAggregator = "AGG_COMPLETE_GHS";
		
	LoeDiscoveryBlock ldb;
	ReportGeneratorPiece rgp;
	ReportDeliveryPiece rdp;
	ConnectionPiece cp;
	
	NegatedControlledGHSBlockSupplier ncgs;
	
	CGHSSupplierUpdaterPiece cghss;
	
	public ControlledGHSBlock(){
		ldb = new LoeDiscoveryBlock();
		rgp = new ReportGeneratorPiece();
		rdp = new ReportDeliveryPiece();
		cp = new ConnectionPiece();
		cghss = new CGHSSupplierUpdaterPiece();
		ncgs = new NegatedControlledGHSBlockSupplier(cghss.getSupplier());
	}

	public Block getBlock() {
		return new RepeatUntilBlock(Integer.MAX_VALUE, 
				new SequenceBlock(
					ldb,
					rgp,
					rdp,
					cghss,
					new IfBlock(
							cghss.getSupplier(), getLOEConnectionAndFragmentUpdateBlock())
					), 
					ncgs);
	}
	
	private Block getLOEConnectionAndFragmentUpdateBlock(){
		return new SequenceBlock(
				cp,
				LeafDiscoveryBlock.createBlock(),
				MISComputationBlock.createBlock()
		);
	}

	/* (non-Javadoc)
	 * @see com.google.common.base.Supplier#get()
	 */
	@Override
	public Boolean get() {
		Logger log = Logger.getLogger(ControlledGHSBlock.class);
		try{
			
			boolean resulto = ((BooleanWritable)new BlockApiHandle().getWorkerSendApi().getAggregatedValue(procedureCompletedAggregator)).get();
			log.info("Paolo finocchio " + resulto);
			return resulto;
		}catch(NullPointerException npe){
			log.info("Paolo strafinocchjio " + false);
		}
		return false;
	}
	
	public static class NegatedControlledGHSBlockSupplier implements Supplier<Boolean> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 6200534517192651871L;
		Supplier<Boolean> supplier;
		
		/**
		 * 
		 */
		public NegatedControlledGHSBlockSupplier(Supplier<Boolean> supplier) {
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
