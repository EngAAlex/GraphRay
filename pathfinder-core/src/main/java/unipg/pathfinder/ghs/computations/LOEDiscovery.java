/**
 * 
 */
package unipg.pathfinder.ghs.computations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.giraph.edge.Edge;
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
		//		if(master.getSuperstep() == 0){
		//			counter++;
		//			return false;
		//		}
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
		}else if(counter == 3){
			master.setComputation(LOEDiscoveryREPORT_DELIVERY.class);
			counter++;
			return false;
		}else if(counter == 4)
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
			if(vertex.getNumEdges() == 0)
				vertexValue.loesDepleted();

			vertexValue.resetLOE();
			Toolbox.disarmPathfinderCandidates(vertex);
			
			if(vertexValue.hasLOEsDepleted())
				return;

			super.compute(vertex, messages);

			if(vertex.getNumEdges() == 1){
				PathfinderVertexID neighbor = vertex.getEdges().iterator().next().getTargetVertexId().copy();
				Toolbox.setEdgeAsBranch(vertex, neighbor);
				sendMessage(neighbor, new ControlledGHSMessage(vertex.getId().copy(), ControlledGHSMessage.FORCE_ACCEPT));
				vertexValue.loesDepleted();
				return;
			}

			//			PathfinderVertexID selectedNeighbor = null;
			Stack<PathfinderVertexID> loes = null;

			//			if(vertexValue.getLOE() == Double.MAX_VALUE){
			//				PathfinderVertexID popped = Toolbox.popPathfinderCandidates(vertex); //check for candidates first
			//
			//				if(popped != null){													
			//					selectedNeighbor = popped;
			//					vertexValue.updateLOE(vertex.getEdgeValue(selectedNeighbor).get());
			//				}else{																//Check for unassigned
			loes = Toolbox.getLOEsForVertex(vertex);

			if(loes == null){
				log.info("depleted loes!");
				vertexValue.loesDepleted();
				return;
			}

			log.info("available loes " + loes.size());

			//					selectedNeighbor = loes.peek();			
			//					vertexValue.setLoeDestination(selectedNeighbor);
			//					vertexValue.updateLOE(vertex.getEdgeValue(selectedNeighbor).get());

			//					if(loes.size() > 0){
			//						Toolbox.setEdgeAsPathfinderCandidate(vertex, selectedNeighbor);
			//						for(PathfinderVertexID pfid : loes)
			//							Toolbox.setEdgeAsPathfinderCandidate(vertex, pfid);
			//					}
			//				}
			//			}else{																//recycle last found neighbor
			//				selectedNeighbor = vertexValue.getLoeDestination(); //SELECTED NEIGHBORS SHOULD BE SAVED AS WELL IN THE VERTEX TYPE
			//			}

			//			log.info("selected " + selectedNeighbor + " testing now with fragment " + vertexValue.getFragmentIdentity());

			for(PathfinderVertexID candidate : loes){ //on all edges with same weight a test message is sent
				log.info("sending test message to " + candidate.get());
				sendMessage(candidate, 
						new ControlledGHSMessage(vertex.getId(), 
								vertexValue.getFragmentIdentity().copy(), 
								ControlledGHSMessage.TEST_MESSAGE));
			}
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
			PathfinderVertexID myFragment = vertexValue.getFragmentIdentity();
			Iterator<ControlledGHSMessage> msgIterator = messages.iterator();
			super.compute(vertex, messages);
			while(msgIterator.hasNext()){				
				ControlledGHSMessage currentMessage = msgIterator.next();
				PathfinderVertexID currentSenderID = currentMessage.getSenderID().copy();
				PathfinderVertexID currentFragment = currentMessage.getFragmentID().copy();
				short currentMessageCode = currentMessage.getStatus();
				log.info("received fragment " + currentFragment);
				if(currentMessageCode == ControlledGHSMessage.TEST_MESSAGE){ 
					if(!myFragment.equals(currentFragment)){ //connection accepted
						log.info("accepted fragment");
						sendMessage(currentSenderID.copy(), 
								new ControlledGHSMessage(vertexId.copy(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ACCEPT_MESSAGE)); //all accept edges will receive also the 
					}else{ //connection refused																										//neighboring fragment ID
						log.info("refused fragment from " + currentSenderID + " removing dge now");
						sendMessage(currentSenderID.copy(), 
								new ControlledGHSMessage(vertexId.copy(), ControlledGHSMessage.REFUSE_MESSAGE));
						//						if(vertexValue.getLoeDestination().equals(currentSenderID)){
						//							vertexValue.resetLOE();
						//							Toolbox.disarmPathfinderCandidates(vertex);
						//						}
						//						if(!vertex.getEdgeValue(currentSenderID).isPathfinderCandidate())
						removeEdgesRequest(vertexId, currentSenderID);
						//						else{
						//							Toolbox.consolidatePathfinder(vertex, currentSenderID);
						//						}
					}
				}else if(currentMessageCode == ControlledGHSMessage.FORCE_ACCEPT){
					Toolbox.setEdgeAsBranch(vertex, currentMessage.getSenderID());
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
			super.compute(vertex, messages);
			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.hasLOEsDepleted() && !vertexValue.isRoot()){					
				sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertex.getId(), ControlledGHSMessage.LOEs_DEPLETED));
				return;
			}
			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			HashMap<PathfinderVertexID, Integer> fragmentFrequencyMap = new HashMap<PathfinderVertexID, Integer>();
			HashMap<PathfinderVertexID, Stack<PathfinderVertexID>> senderFragmentMap = new HashMap<PathfinderVertexID, Stack<PathfinderVertexID>>();

			while(msgs.hasNext()){
				ControlledGHSMessage currentMessage = msgs.next();
				PathfinderVertexID senderID = currentMessage.getSenderID().copy();
				PathfinderVertexID remoteFragment = currentMessage.getFragmentID().copy();
				short currentMessageCode = currentMessage.getStatus();
				switch(currentMessageCode){
				case ControlledGHSMessage.REFUSE_MESSAGE:
					log.info("Received a refuse message from " + senderID + " removing edge now");
					//					if(!vertex.getEdgeValue(senderID).isPathfinderCandidate())
					removeEdgesRequest(vertexId, senderID); 
					//					vertexValue.resetLOE(); 
					//					Toolbox.disarmPathfinderCandidates(vertex);
					break;						
				case ControlledGHSMessage.ACCEPT_MESSAGE:

					if(!fragmentFrequencyMap.containsKey(remoteFragment))
						fragmentFrequencyMap.put(remoteFragment, 1);
					else
						fragmentFrequencyMap.put(remoteFragment, fragmentFrequencyMap.get(remoteFragment) + 1);

					if(!senderFragmentMap.containsKey(remoteFragment))
						senderFragmentMap.put(remoteFragment, new Stack<PathfinderVertexID>());
					senderFragmentMap.get(remoteFragment).push(senderID);
					break;
				}
			}

			if(fragmentFrequencyMap.size() == 0)
				return;			

			Set<Entry<PathfinderVertexID, Integer>> entrySet = fragmentFrequencyMap.entrySet();
			PathfinderVertexID maxIndex = null;
			int max = Integer.MIN_VALUE;
			for(Entry<PathfinderVertexID, Integer> e : entrySet)
				if(e.getValue() > max){
					max = e.getValue();
					maxIndex = e.getKey();
				}

			log.info("chose fragment " + maxIndex + " with " + max + " occurrencies");

			Toolbox.setMultipleEdgesAsCandidates(vertex, senderFragmentMap.get(maxIndex));

			PathfinderVertexID selectedNeighbor = senderFragmentMap.get(maxIndex).peek();
			vertexValue.setLoeDestination(selectedNeighbor);
			vertexValue.updateLOE(vertex.getEdgeValue(selectedNeighbor).get());
			log.info("selected neighbor " + selectedNeighbor);

			if(!vertexValue.isRoot()){
				log.info("Reporting to root " + vertexValue.getFragmentIdentity());
				sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertexId, vertexValue.getLOE(), ControlledGHSMessage.REPORT_MESSAGE));
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
			super.compute(vertex, messages);
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
				if(currentStatus == ControlledGHSMessage.LOEs_DEPLETED){
					log.info("Loes depleted from " + currentMessage.getSenderID());
					continue; 
				}
				PathfinderVertexID currentSenderID = currentMessage.getSenderID().copy();
				double currentValue = currentMessage.get();
				if(currentValue < minLOE){
					minLOE = currentValue;
					minLOEDestination = currentSenderID;
				}
			}
			log.info("manifacturing delivery " + vertexValue.getLOE() + " min " + minLOE);
			if(vertexValue.getLOE() != Double.MAX_VALUE && vertexValue.getLOE() <= minLOE){
				//				Toolbox.armPathfinderCandidates(vertex);
				minLOE = vertexValue.getLOE();
				minLOEDestination = vertexValue.getLoeDestination();

				log.info("Root wins " + minLOE + " " + minLOEDestination);
				//				vertex.getEdgeValue(minLOEDestination).setAsBranchEdge();
				//				log.info("Connected " + minLOEDestination);
			}else{
				Toolbox.disarmPathfinderCandidates(vertex);
				vertexValue.updateLOE(minLOE);
				vertexValue.setLoeDestination(minLOEDestination);
			}
			if(minLOE != Double.MAX_VALUE){
				log.info("sending connect message to " + minLOEDestination);
				sendMessage(minLOEDestination, new ControlledGHSMessage(vertexId, vertexValue.getFragmentIdentity(), ControlledGHSMessage.CONNECT_MESSAGE));
				aggregate(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, new BooleanWritable(false));

				Iterable<PathfinderVertexID> branches = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH);

				if(branches != null)
					for(PathfinderVertexID e : branches)
						if(!e.equals(minLOEDestination))
							sendMessage(e, new ControlledGHSMessage(vertexId, ControlledGHSMessage.REFUSE_MESSAGE));
			}
			//else
			//				log.info("Aggregated a true mannaggia");
			//			vertexValue.resetLOE();
		}		
	}


}
