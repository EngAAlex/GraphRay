/**
 * 
 */
package unipg.pathfinder.ghs.computations;

import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.utils.Toolbox;

/**
 * @author spark
 *
 */
public class LOEDiscovery {
	
	protected static Logger log = Logger.getLogger(LOEDiscovery.class);

	MasterCompute master;
	int counter = 0;
	
	/**
	 * 
	 */
	public LOEDiscovery(MasterCompute master) {
		this.master = master;
	}
	
	public boolean compute(){
		if(counter == 0){
			counter++;
			master.setComputation(LOEDiscoveryTEST.class);
			return false;
		}else if(counter == 1){
			master.setComputation(LOEDiscoveryTEST_REPLY.class);
			counter++;
			return false;
		}else if(counter == 2){
			master.setComputation(LOEDiscoveryREPORT_GENERATION.class);
			counter++;
			return false;
		}else if(counter == 4){
			master.setComputation(LOEDiscoveryREPORT_DELIVERY.class);
			counter++;
			return false;
		}else if(counter == 5)
			counter = 0;
		return true;
	}
	
	/**
	 * @author spark
	 *
	 */
	public static class LOEDiscoveryTEST extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage> {

		Logger log = Logger.getLogger(this.getClass());

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.hasLOEsDepleted())
				return;

			PathfinderVertexID selectedNeighbor = null;
			Stack<PathfinderVertexID> loes = null;

			if(vertexValue.getLOE() == Double.MAX_VALUE){
				loes = Toolbox.getLOEsForVertex(vertex, PathfinderEdgeType.UNASSIGNED);

				if(loes == null){
					vertexValue.loesDepleted();
					return;
				}

				selectedNeighbor = loes.pop();			
				vertexValue.setLoeDestination(selectedNeighbor);

				for(PathfinderVertexID pfid : loes)
					vertex.getEdgeValue(pfid).setAsPathfinderCandidate();
			}else
				selectedNeighbor = vertexValue.getLoeDestination(); //SELECTED NEIGHBORS SHOULD BE SAVED AS WELL IN THE VERTEX TYPE

			sendMessage(selectedNeighbor, 
					new ControlledGHSMessage(vertex.getId(), 
							vertexValue.getFragmentIdentity(), 
							vertexValue.getDepth(), 
							ControlledGHSMessage.TEST_MESSAGE));
		}
	}

	public static class LOEDiscoveryTEST_REPLY extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexType vertexValue = vertex.getValue();
			PathfinderVertexID myId = vertexId;
			PathfinderVertexID myFragment = vertexValue.getFragmentIdentity();
			Iterator<ControlledGHSMessage> msgIterator = messages.iterator();
			while(msgIterator.hasNext()){
				ControlledGHSMessage currentMessage = msgIterator.next();
				PathfinderVertexID currentSenderID = currentMessage.getSenderID();
				PathfinderVertexID currentFragment = currentMessage.getFragmentID();
				short currentMessageCode = currentMessage.getStatus();
				//					int msgDepth = currentMessage.getDepth();
				if(currentMessageCode == ControlledGHSMessage.TEST_MESSAGE){ 
					if(!myFragment.equals(currentFragment)){ //connection accepted
						sendMessage(currentSenderID, 
								new ControlledGHSMessage(myId, ControlledGHSMessage.ACCEPT_MESSAGE));
					}else{ //connection refused
						sendMessage(currentSenderID, 
								new ControlledGHSMessage(myId, ControlledGHSMessage.REFUSE_MESSAGE));
						if(vertexValue.getLoeDestination() == currentSenderID)
							vertexValue.resetLOE();
						if(!vertex.getEdgeValue(currentSenderID).isPathfinderCandidate())
							removeEdgesRequest(vertexId, currentSenderID);							
					}
				}
			}
		}		
	}

	public static class LOEDiscoveryREPORT_GENERATION extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.hasLOEsDepleted() && !vertexValue.isRoot()){					
				sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertex.getId(), ControlledGHSMessage.LOEs_DEPLETED));
				return;
			}
			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			while(msgs.hasNext()){
				ControlledGHSMessage currentMessage = msgs.next();
				PathfinderVertexID currentFragment = currentMessage.getFragmentID();
				short currentMessageCode = currentMessage.getStatus();
				switch(currentMessageCode){
				case ControlledGHSMessage.REFUSE_MESSAGE:
					if(!vertex.getEdgeValue(currentFragment).isPathfinderCandidate())
						removeEdgesRequest(vertexId, currentFragment); vertexValue.resetLOE(); break;						
				case ControlledGHSMessage.ACCEPT_MESSAGE:
					if(!vertexValue.isRoot())
						sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertexId, vertexValue.getLOE(), ControlledGHSMessage.REPORT_MESSAGE));
					break;
				}
			}			
		}	
	}

	public static class LOEDiscoveryREPORT_DELIVERY extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage> {

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			log.info("Startedloechoice");
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
				Toolbox.armPathfinderCandidates(vertex);
				vertex.getEdgeValue(minLOEDestination).setAsBranchEdge();
				minLOE = vertexValue.getLOE();
				minLOEDestination = vertexValue.getLoeDestination();
			}else{
				Toolbox.disarmPathfinderCandidates(vertex);
			}
			boolean paolo = true;
			if(minLOE != Double.MAX_VALUE){
				sendMessage(minLOEDestination, new ControlledGHSMessage(vertexId, vertexValue.getFragmentIdentity(), ControlledGHSMessage.CONNECT_MESSAGE));
				aggregate(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, new BooleanWritable(false));
				log.info("Aggregated a false mannaggia");
				paolo = false;
			}
			if(paolo)
				Logger.getLogger(getClass()).info("Aggregated a true mannaggia");
			vertexValue.resetLOE();
		}		
	}


}
