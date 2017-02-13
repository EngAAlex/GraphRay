/**
 * 
 */
package unipg.pathfinder.blocks_framework.boruvka.pieces;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.edge.EdgeFactory;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.blocks_framework.mst.blocks.MSTBlockWithApiHandle;
import unipg.pathfinder.utils.Toolbox;

/**
 * @author spark
 *
 */
public class BoruvkaRootUpdatePiece extends MSTBlockWithApiHandle {	
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.piece.DefaultParentPiece#getVertexSender(org.apache.giraph.block_app.framework.api.BlockWorkerSendApi, java.lang.Object)
	 */
	@Override
	public VertexSender<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> getVertexSender( //roots inform their border vertices that the root is being updated
			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi,
			Object executionStage) {
		return (vertex) -> {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.isRoot() && !vertexValue.boruvkaStatus()){ //the vertex is a deactivated root and must be updated
				vertexValue.setRoot(false); //the vertex will remain silent from now on
				workerApi.sendMessageToMultipleEdges(Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH, PathfinderEdgeType.DUMMY).iterator(), 
						new ControlledGHSMessage(vertex.getId(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ROOT_UPDATE));
			}
		};
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.block_app.framework.piece.AbstractPiece#getVertexReceiver(org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi, java.lang.Object)
	 */
	@Override
	public VertexReceiver<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> getVertexReceiver( //border vertices update the edges and everything
			BlockWorkerReceiveApi<PathfinderVertexID> workerApi, Object executionStage) {
		return (vertex, messages) -> {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(!vertexValue.boruvkaStatus())
				return;
			if(vertex.getValue().isRoot()){
//				getBlockApiHandle().getWorkerSendApi().aggregate(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator, new IntWritable(1));
				return;
			}
			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			PathfinderVertexID newFragmentID = msgs.next().getFragmentID();
			Iterator<PathfinderVertexID> dummyEdges = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.DUMMY).iterator();
			while(dummyEdges.hasNext()){
				PathfinderVertexID currentDummy = dummyEdges.next();				//old pair of dummies are removed
				getBlockApiHandle().getWorkerSendApi().removeEdgesRequest(vertex.getId(), currentDummy);
				getBlockApiHandle().getWorkerSendApi().removeEdgesRequest(currentDummy, vertex.getId());
			}			
			//			if(msgs.hasNext()) what if is not the only message?
			//				throw new Exception();
			vertexValue.setFragmentIdentity(newFragmentID);
			getBlockApiHandle().getWorkerSendApi().addEdgeRequest(vertex.getId(), EdgeFactory.create(newFragmentID, new PathfinderEdgeType(PathfinderEdgeType.DUMMY))); //new pair is created
			getBlockApiHandle().getWorkerSendApi().addEdgeRequest(newFragmentID, EdgeFactory.create(vertex.getId(), new PathfinderEdgeType(PathfinderEdgeType.DUMMY)));
		};
	}


}
