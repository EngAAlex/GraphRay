/**
 * 
 */
package unipg.pathfinder.mst.blocks;

import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.conf.GiraphConfiguration;

import unipg.pathfinder.mst.boruvka.blocks.BoruvkaBlock;
import unipg.pathfinder.mst.ghs.blocks.ControlledGHSBlock;

/**
 * @author spark
 *
 */
public class MSTPathfinderBlock extends AbstractMSTBlockFactory{

	BlockApiHandle bah;
	
	ControlledGHSBlock cGHSblock;
	BoruvkaBlock boruvkaBlock;
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	public MSTPathfinderBlock() {
		bah = new BlockApiHandle();
		cGHSblock = new ControlledGHSBlock(bah.getWorkerSendApi());
		boruvkaBlock = new BoruvkaBlock(bah.getWorkerSendApi());
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createBlock(org.apache.giraph.conf.GiraphConfiguration)
	 */
	@Override
	public Block createBlock(GiraphConfiguration conf) {
		return new SequenceBlock(
				cGHSblock.getBlock(),
				boruvkaBlock.getBlock()
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
