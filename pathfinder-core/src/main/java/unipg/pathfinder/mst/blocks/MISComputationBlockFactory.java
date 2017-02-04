/**
 * 
 */
package unipg.pathfinder.mst.blocks;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.utils.Toolbox;

public class MISComputationBlockFactory  extends AbstractMSTBlockFactory{

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createBlock(org.apache.giraph.conf.GiraphConfiguration)
	 */
	@Override
	public Block createBlock(GiraphConfiguration conf) {
		return new SequenceBlock(
				Pieces.<PathfinderVertexID, PathfinderVertexType, Writable, DoubleWritable>sendMessageToNeighbors( //MIS COMPUTATION
						"MISComputation", 
						DoubleWritable.class, 
						(vertex) -> {
							PathfinderVertexType vertexValue = vertex.getValue();
							if(vertexValue.getDepth() == -1){ 
								vertexValue.setMISValue(Math.random());
								return new DoubleWritable(vertexValue.getMISValue());
							}else
								return null;
						},
						(vertex, messages) -> {
							PathfinderVertexType vertexValue = vertex.getValue();
							int vertexDepth = vertexValue.getDepth();
							if(vertexDepth != -1){
								if(vertexDepth == 1)
									vertexValue.setRoot(true);
								return;
							}
							boolean foundSmaller = false;
							double myValue = vertexValue.getMISValue();
							Iterator<DoubleWritable> it = messages.iterator();			
							while(it.hasNext()){
								if(it.next().get() < myValue){
									foundSmaller = true;
									break;
								}
							}
							vertexValue.setRoot(!foundSmaller);
							vertexValue.resetDepth();							
						}),
				Pieces.<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, LongWritable>sendMessage( //FRAGMENT RECONSTRUCTION FROM ROOTS
						"FragmentReconstruction", LongWritable.class, 
						(vertex) -> {
							PathfinderVertexType vertexValue = vertex.getValue();
							if(vertexValue.isRoot()){
								long fragmentIdentity = vertex.getId().get();
								vertexValue.setFragmentIdentity(fragmentIdentity);
								return new LongWritable(fragmentIdentity);
							}else
								return null;
						},
						(vertex) -> {
							return Toolbox.getBranchEdgesForVertex(vertex).iterator();
						}, 
						(vertex, messages) -> {
							PathfinderVertexType vertexValue = vertex.getValue();
							if(!vertexValue.isRoot()){
								Iterator<LongWritable> msgs = messages.iterator();
								vertexValue.setFragmentIdentity(msgs.next().get());
							}
						})
				);
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createExecutionStage(org.apache.giraph.conf.GiraphConfiguration)
	 */
	@Override
	public Object createExecutionStage(GiraphConfiguration arg0) {
		// TODO Auto-generated method stub
		return "MIS Computation";
	}

	
}
