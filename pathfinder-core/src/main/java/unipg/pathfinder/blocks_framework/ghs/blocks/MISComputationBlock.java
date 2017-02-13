/**
 * 
 */
package unipg.pathfinder.blocks_framework.ghs.blocks;

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

public class MISComputationBlock{

	public static Block createBlock() {
		return new SequenceBlock(
				Pieces.<PathfinderVertexID, PathfinderVertexType, Writable, DoubleWritable>sendMessageToNeighbors( //MIS COMPUTATION
						"MISComputation", 
						DoubleWritable.class, 
						(vertex) -> {
							PathfinderVertexType vertexValue = vertex.getValue();
							vertexValue.setRoot(false);							
							if(vertexValue.getDepth() == -1){ 
								vertexValue.setMISValue(Math.random());
								return new DoubleWritable(vertexValue.getMISValue());
							}else if(vertexValue.getDepth() == 1)
								vertexValue.setRoot(true);
							return null;
						},
						(vertex, messages) -> {
							PathfinderVertexType vertexValue = vertex.getValue();
							int vertexDepth = vertexValue.getDepth();
							if(vertexDepth != -1)
								return;							
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
				Pieces.<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, PathfinderVertexID>sendMessage( //FRAGMENT RECONSTRUCTION FROM ROOTS
						"FragmentReconstruction", PathfinderVertexID.class, 
						(vertex) -> {
							PathfinderVertexType vertexValue = vertex.getValue();
							if(vertexValue.isRoot()){
								PathfinderVertexID fragmentIdentity = vertex.getId();
								vertexValue.setFragmentIdentity(fragmentIdentity);
								return fragmentIdentity.copy();
							}else
								return null;
						},
						(vertex) -> {
							return Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH).iterator();
						}, 
						(vertex, messages) -> {
							PathfinderVertexType vertexValue = vertex.getValue();
							if(!vertexValue.isRoot()){
								Iterator<PathfinderVertexID> msgs = messages.iterator();
								vertexValue.setFragmentIdentity(msgs.next().copy());
							}
						})
				);
	}
	
}
