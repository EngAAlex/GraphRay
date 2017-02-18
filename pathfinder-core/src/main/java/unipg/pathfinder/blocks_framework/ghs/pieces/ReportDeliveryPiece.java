/**
 * 
 */
package unipg.pathfinder.blocks_framework.ghs.pieces;

import java.util.Iterator;

import org.apache.giraph.function.Supplier;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.blocks_framework.mst.blocks.MSTBlockWithApiHandle;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.utils.Toolbox;

/**
 * @author spark
 *
 */
public class ReportDeliveryPiece extends MSTBlockWithApiHandle{
		
	/**
	 * 
	 */
	private static final long serialVersionUID = -2742799699201974030L;

	/**
	 * @param workerSendApi
	 */
//	public ReportDeliveryPiece(
//			BlockWorkerSendApi<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, ControlledGHSMessage> workerSendApi) {
//		super(workerSendApi);
//	}

	public ConsumerWithVertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, Iterable<ControlledGHSMessage>> getLOEChoiceVertexConsumer(){
		return (vertex, messages) -> {
//			log.info("Startedloechoice");
			PathfinderVertexType vertexValue = vertex.getValue();
			if(!vertexValue.isRoot()) //only roots will react now, and I'm sure only them will have messages incoming
				return;
			PathfinderVertexID vertexId = vertex.getId();
			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			PathfinderVertexID minLOEDestination = null;
			double minLOE = Double.MAX_VALUE;
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
			if(vertexValue.getLOE() < minLOE){
//				Toolbox.armPathfinderCandidates(vertex);
				vertex.getEdgeValue(minLOEDestination).setAsBranchEdge();
				minLOE = vertexValue.getLOE();
				minLOEDestination = vertexValue.getLoeDestination();
			}else{
				Toolbox.disarmPathfinderCandidates(vertex);
			}
			boolean paolo = true;
			if(minLOE != Double.MAX_VALUE){
				getBlockApiHandle().getWorkerSendApi().sendMessage(minLOEDestination, new ControlledGHSMessage(vertexId, vertexValue.getFragmentIdentity(), ControlledGHSMessage.CONNECT_MESSAGE));
				getBlockApiHandle().getWorkerSendApi().aggregate(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, new BooleanWritable(false));
//				log.info("Aggregated a false mannaggia");
				paolo = false;
			}
			if(paolo)
				Logger.getLogger(getClass()).info("Aggregated a true mannaggia");
			vertexValue.resetLOE();
		};
	}

}
