/**
 * 
 */
package unipg.pathfinder.mst.boruvka.blocks;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.mst.blocks.LoeDiscoveryBlock;
import unipg.pathfinder.mst.boruvka.pieces.BoruvkaMessageDeliveryPiece;
import unipg.pathfinder.mst.boruvka.pieces.BoruvkaRootUpdatePiece;

/**
 * @author spark
 *
 */
public class BoruvkaBlock implements Supplier<Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8754960762132943190L;

	BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi;
	
	LoeDiscoveryBlock ldb;
	BoruvkaMessageDeliveryPiece bmdp;
	BoruvkaRootUpdatePiece brup;
		
	/**
	 * 
	 */
	public BoruvkaBlock(BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi){
		this.workerSendApi = workerSendApi;
		
		bmdp = new BoruvkaMessageDeliveryPiece(workerSendApi);
		ldb = new LoeDiscoveryBlock(workerSendApi);
		brup = new BoruvkaRootUpdatePiece(workerSendApi);
	}
	
	public Block getBlock(){
		return new SequenceBlock(
				boruvkaSetupBlock(),
				new RepeatUntilBlock(0, 
				new SequenceBlock(
					ldb.getLOEDiscoveryBlock(),
					bmdp,
					brup),					
				this)				
				);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.function.Supplier#get()
	 */
	@Override
	public Boolean get() {
		return ((IntWritable)workerSendApi.getAggregatedValue(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator)).get() == 1;
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
