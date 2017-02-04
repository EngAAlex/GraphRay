/**
 * 
 */
package unipg.pathfinder.mst.blocks;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class ControlledGHSBlockFactory extends AbstractMSTBlockFactory {

	public static final String procedureCompletedAggregator = "AGG_COMPLETE_GHS";
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createBlock(org.apache.giraph.conf.GiraphConfiguration)
	 */
	public Block createBlock(GiraphConfiguration arg0) {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createExecutionStage(org.apache.giraph.conf.GiraphConfiguration)
	 */
	public Object createExecutionStage(GiraphConfiguration arg0) {
		return AbstractMSTBlockFactory.controlledGHSExecutionStage;
	}
	

}
