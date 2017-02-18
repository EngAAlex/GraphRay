/**
 * 
 */
package unipg.pathfinder.ghs.computations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.utils.Toolbox;


public class GHSComputations{

	protected static Logger log = Logger.getLogger(GHSComputations.class);


	public static class LOEConnection extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage> {

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			Iterator<ControlledGHSMessage> msgs = messages.iterator();					
			//			if(!msgs.hasNext()/* && vertex.getValue().isRoot()*/){
			//				Toolbox.disarmPathfinderCandidates(vertex);
			//				vertexValue.resetLOE();
			//				return;
			//			}
			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexID myLOEDestination = vertexValue.getLoeDestination();
			PathfinderVertexID myfragmentIdentity = vertexValue.getFragmentIdentity();

			boolean proceed = true;
			PathfinderVertexID connectionCandidate = null;
			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID msgFragmentIdentity = current.getFragmentID();
				PathfinderVertexID msgSender = current.getSenderID();
				short msgStatus = current.getStatus();
				if(msgStatus == ControlledGHSMessage.REFUSE_MESSAGE){
					log.info("Received a refuse message");
					proceed = false;
					break;
				}
				if(msgFragmentIdentity.equals(myfragmentIdentity)){ //CONNECT MESSAGE DID NOT CROSS THE FRAGMENT BORDER
					log.info("vertex " + vertex.getId() + " forwarding message to " + myLOEDestination);
					sendMessage(myLOEDestination, new ControlledGHSMessage(vertexId, myfragmentIdentity, ControlledGHSMessage.CONNECT_MESSAGE));
					aggregate(MSTPathfinderMasterCompute.messagesLeftAggregator, new BooleanWritable(false));
				} 
				//				else { 
				if(myLOEDestination != null && (myLOEDestination.equals(msgSender) || vertex.getEdgeValue(msgSender).isPathfinderCandidate())){ //CONNECT MESSAGE CROSSED THE FRAGMENT BORDER
					log.info("Attempting to connect " + msgSender);
					connectionCandidate = msgSender.copy();
				}
				//				}
			}
			if(proceed && connectionCandidate != null){
				Toolbox.setEdgeAsBranch(vertex, connectionCandidate);				
				if(!connectionCandidate.equals(myLOEDestination)){
					Toolbox.setEdgeAsPathfinderCandidate(vertex, myLOEDestination);
					sendMessage(connectionCandidate, new ControlledGHSMessage(vertexId, ControlledGHSMessage.CONNECT_MESSAGE));
					Toolbox.armRemotePathfinderCandidates(this, vertex);					
					log.info("Connected with " + connectionCandidate);
				}else{
					Toolbox.armPathfinderCandidates(vertex);					
					log.info("Connected with " + myLOEDestination);
				}
			}
		}
	}

	public static class LOEConnectionEncore extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage> { //this is necessary to ensure that each vertex
		//is aware of its branches

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			if(!msgs.hasNext())
				return;
			PathfinderVertexID myLOEDestination = vertexValue.getLoeDestination();
			ControlledGHSMessage msg = msgs.next();
			if(msg.getStatus() == ControlledGHSMessage.CONNECT_MESSAGE && msg.getSenderID().equals(myLOEDestination)){
				Toolbox.setEdgeAsBranch(vertex, myLOEDestination);
				log.info("Connected with " + myLOEDestination);
			}
		}

	}
}
