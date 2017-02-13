/**
 * 
 */
package unipg.pathfinder.blocks_framework.boruvka.blocks;

import java.util.Iterator;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.blocks_framework.boruvka.pieces.BoruvkaMessageDeliveryPiece;
import unipg.pathfinder.blocks_framework.boruvka.pieces.BoruvkaRootUpdatePiece;
import unipg.pathfinder.blocks_framework.boruvka.pieces.BoruvkaSupplierUpdaterPiece;
import unipg.pathfinder.blocks_framework.mst.blocks.LoeDiscoveryBlock;
import unipg.pathfinder.blocks_framework.mst.blocks.MSTBlockWithApiHandle;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.suppliers.Suppliers.BoruvkaSupplier;

/**
 * @author spark
 *
 */
public class BoruvkaBlock extends MSTBlockWithApiHandle implements Supplier<Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8754960762132943190L;

	LoeDiscoveryBlock ldb;
	BoruvkaMessageDeliveryPiece bmdp;
	BoruvkaRootUpdatePiece brup;	
	BoruvkaSupplierUpdaterPiece bsup;

	BoruvkaSupplier bs;
	/**
	 * 
	 */
	public BoruvkaBlock(){
		super();
		bmdp = new BoruvkaMessageDeliveryPiece();
		ldb = new LoeDiscoveryBlock();
		brup = new BoruvkaRootUpdatePiece();
		bsup = new BoruvkaSupplierUpdaterPiece();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.piece.AbstractPiece#registerAggregators(org.apache.giraph.block_app.framework.api.BlockMasterApi)
	 */
	@SuppressWarnings("deprecation")
	@Override
	public void registerAggregators(BlockMasterApi masterApi) throws InstantiationException, IllegalAccessException {
		masterApi.registerAggregator(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, BooleanAndAggregator.class);
	}

	public Block getBlock(){
		return new SequenceBlock(
				boruvkaSetupBlock(),
				new RepeatUntilBlock(Integer.MAX_VALUE, 
						new SequenceBlock(
								ldb,
								bmdp,
								brup,
								bsup),					
						bsup.getSupplier())				
				);
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.function.Supplier#get()
	 */
	@Override
	public Boolean get() {
		try{
			return ((IntWritable)getBlockApiHandle().getWorkerSendApi().getAggregatedValue(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator)).get() == 1;
		}catch(NullPointerException npe){
			
		}
		return false;
	}

	private static Block boruvkaSetupBlock(){
		return Pieces.<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType>forAllVertices("BoruvkaSetup", 
				(vertex) -> {
					vertex.getValue().resetLOE();
					if(!vertex.getValue().isRoot()){
						PathfinderVertexID fragmentIdentity = vertex.getValue().getFragmentIdentity();
						Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
						while(edges.hasNext()){
							Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
							if(!current.getTargetVertexId().equals(fragmentIdentity))
								current.getValue().revertToUnassigned();
						}
					}
				});
	}


}
