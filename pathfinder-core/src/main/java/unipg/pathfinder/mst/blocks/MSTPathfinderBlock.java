/**
 * 
 */
package unipg.pathfinder.mst.blocks;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.boruvka.blocks.BoruvkaBlock;
import unipg.pathfinder.ghs.blocks.ControlledGHSBlock;

/**
 * @author spark
 *
 */
public class MSTPathfinderBlock extends AbstractMSTBlockFactory{

//	BlockApiHandle bah;
	
	ControlledGHSBlock cGHSblock;
	BoruvkaBlock boruvkaBlock;
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	public MSTPathfinderBlock() {
//		bah = new BlockApiHandle();
		cGHSblock = new ControlledGHSBlock();
		boruvkaBlock = new BoruvkaBlock();
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createBlock(org.apache.giraph.conf.GiraphConfiguration)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Block createBlock(GiraphConfiguration conf) {
		
		return new SequenceBlock(
				cGHSblock.getBlock(),
				boruvkaBlock.getBlock(),			
				Pieces.<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType>forAllVertices("DummyEdgesCleanup", 
						(vertex) -> {
							Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
							while(edges.hasNext()){
								Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
								if(current.getValue().isDummy())
									new BlockApiHandle().getWorkerSendApi().removeEdgesRequest(vertex.getId(), current.getTargetVertexId());
							}
						}
				)
		);
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createExecutionStage(org.apache.giraph.conf.GiraphConfiguration)
	 */
	@Override
	public Object createExecutionStage(GiraphConfiguration arg0) {
		return "Pathfinder MST";
	}

}
