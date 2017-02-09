/**
 * 
 */
package unipg.pathfinder.mst.blocks;

import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.block_app.framework.block.BlockWithApiHandle;
import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.function.Consumer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author spark
 *
 */
public class MSTBlockWithApiHandle<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable, S extends Object> extends Piece<I, V, E, M, S> implements BlockWithApiHandle {

	private BlockApiHandle bah;
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.block.Block#forAllPossiblePieces(org.apache.giraph.function.Consumer)
	 */
	@Override
	public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) {
		super.forAllPossiblePieces(consumer);
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.block.BlockWithApiHandle#getBlockApiHandle()
	 */
	@Override
	public BlockApiHandle getBlockApiHandle() {
		if(bah == null)
			bah = new BlockApiHandle();
		return bah;
	}

}
