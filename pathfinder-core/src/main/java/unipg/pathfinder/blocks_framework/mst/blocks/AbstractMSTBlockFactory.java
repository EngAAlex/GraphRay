/**
 * 
 */
package unipg.pathfinder.blocks_framework.mst.blocks;

import org.apache.giraph.block_app.framework.AbstractBlockFactory;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public abstract class AbstractMSTBlockFactory extends AbstractBlockFactory<Object> {
	
	public static final String controlledGHSExecutionStage = "CtrlGHS";	
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.AbstractBlockFactory#getVertexValueClass(org.apache.giraph.conf.GiraphConfiguration)
	 */
	@Override
	protected Class<? extends Writable> getVertexValueClass(GiraphConfiguration conf) {
		return PathfinderVertexType.class;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.AbstractBlockFactory#getEdgeValueClass(org.apache.giraph.conf.GiraphConfiguration)
	 */
	@Override
	protected Class<? extends Writable> getEdgeValueClass(GiraphConfiguration conf) {
		return PathfinderEdgeType.class;
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.AbstractBlockFactory#getVertexIDClass(org.apache.giraph.conf.GiraphConfiguration)
	 */
	@Override
	protected Class<? extends WritableComparable> getVertexIDClass(GiraphConfiguration conf) {
		return PathfinderVertexID.class;
	}
	
}
