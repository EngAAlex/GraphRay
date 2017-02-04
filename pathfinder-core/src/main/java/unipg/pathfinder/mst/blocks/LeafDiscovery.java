/**
 * 
 */
package unipg.pathfinder.mst.blocks;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.io.ByteWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.utils.Toolbox;

/**
 * @author spark
 *
 */
public class LeafDiscovery extends AbstractMSTBlockFactory {

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createBlock(org.apache.giraph.conf.GiraphConfiguration)
	 */
	@Override
	public Block createBlock(GiraphConfiguration conf) {
		Block leafDiscoveryBlock = Pieces.<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ByteWritable>sendMessage("LeafDiscovery", ByteWritable.class, 
				(vertex) -> {
					PathfinderVertexType vertexValue = vertex.getValue();
					if(vertexValue.isIsolated())
						return null;
					if(vertexValue.getDepth() == -1){
						if(vertexValue.isLeaf()){
							vertexValue.setDepth((byte) 0);
							return new ByteWritable((byte) 0);
						}else 
							return new ByteWritable((byte) 1);
					}else
						return null;
				},
				(vertex) -> {
					return Toolbox.getBranchEdgesForVertex(vertex).iterator();
				},
				(vertex, messages) -> {
					PathfinderVertexType vertexValue = vertex.getValue();
					if(vertexValue.getDepth() != -1)
						return;
					Iterator<ByteWritable> msgs = messages.iterator();
					switch(msgs.next().get()){
					case (byte) 0: 
						vertexValue.setDepth((byte) 0);
						break;
					case (byte) 1: 
						vertexValue.setDepth((byte) 1);
						break;
					}
				});
		return new RepeatBlock(2, leafDiscoveryBlock);
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createExecutionStage(org.apache.giraph.conf.GiraphConfiguration)
	 */
	@Override
	public Object createExecutionStage(GiraphConfiguration arg0) {
		return "Leaf discovery";
	}

}
