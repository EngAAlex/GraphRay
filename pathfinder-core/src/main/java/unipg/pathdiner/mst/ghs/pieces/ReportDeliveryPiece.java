/**
 * 
 */
package unipg.pathdiner.mst.ghs.pieces;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.hadoop.io.BooleanWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.mst.blocks.MSTPieceWithWorkerApi;

/**
 * @author spark
 *
 */
public class ReportDeliveryPiece extends MSTPieceWithWorkerApi {

	/**
	 * @param workerSendApi
	 */
	public ReportDeliveryPiece(
			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi) {
		super(workerSendApi);
	}

	public ConsumerWithVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterable<ControlledGHSMessage>> getLOEChoiceVertexConsumer(
			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerApi){
		return (vertex, messages) -> {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(!vertexValue.isRoot()) //only roots will react now, and I'm sure only them will have messages incoming
				return;
			PathfinderVertexID vertexId = vertex.getId();
			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			PathfinderVertexID minLOEDestination = vertexValue.getLoeDestination();
			double minLOE = vertexValue.getLOE();
			while(msgs.hasNext()){
				ControlledGHSMessage currentMessage = msgs.next();
				short currentStatus = currentMessage.getStatus();
				if(currentStatus == ControlledGHSMessage.LOEs_DEPLETED)
					continue;
				PathfinderVertexID currentSenderID = currentMessage.getSenderID();
				double currentValue = currentMessage.get();
				if(currentValue < minLOE){
					minLOE = currentValue;
					minLOEDestination = currentSenderID;
				}
			}
			if(minLOE != Double.MAX_VALUE){
				workerApi.sendMessage(minLOEDestination, new ControlledGHSMessage(vertexId, vertexValue.getFragmentIdentity(), ControlledGHSMessage.CONNECT_MESSAGE));
				workerApi.aggregate(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, new BooleanWritable(false));
			}
		};
	}
}
