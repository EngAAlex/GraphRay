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

import pathfinder.utils.algorithms.mis.MISPieces;
import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class ControlledGHSBlockFactory extends AbstractMSTBlockFactory {

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createBlock(org.apache.giraph.conf.GiraphConfiguration)
	 */
	public Block createBlock(GiraphConfiguration arg0) {
		return new SequenceBlock(
				Pieces.<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage>sendMessage("StartingSequence", ControlledGHSMessage.class, 
							(vertex) -> {
								Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
								double min = Double.MAX_VALUE;
								long selectedNeighbor;
								PathfinderVertexType pvt = vertex.getValue();
								while(edges.hasNext()){
									Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
									double currentEdgeValue = current.getValue().get();
									long currentNeighbor = current.getTargetVertexId().get();
									if(currentEdgeValue < min){
										min = currentEdgeValue;
										selectedNeighbor = currentNeighbor;
									}
								}
								if(min != Double.MAX_VALUE){
									pvt.updateLOE(min);
									return new ControlledGHSMessage(vertex.getId().get(), ControlledGHSMessage.TEST_MESSAGE);
								}else
									return null;
							},
							(vertex) -> {
								
							},
							(vertex, messages) -> {
								
							}),
				new MISPieces.MISComputation<PathfinderVertexID, PathfinderVertexType>(),
				Pieces.forAllVertices("Fragments Update", 
						(vertex) -> {
							
						})
		);
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.BlockFactory#createExecutionStage(org.apache.giraph.conf.GiraphConfiguration)
	 */
	public Object createExecutionStage(GiraphConfiguration arg0) {
		return AbstractMSTBlockFactory.controlledGHSExecutionStage;
	}
	

}
