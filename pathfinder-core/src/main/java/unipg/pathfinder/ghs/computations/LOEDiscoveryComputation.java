/**
 * 
 */
package unipg.pathfinder.ghs.computations;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.utils.Toolbox;



public abstract class LOEDiscoveryComputation extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

	
	
	protected short loesToDiscover;
	protected short loesToRemove;

	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.bsp.CentralizedServiceWorker, org.apache.giraph.worker.WorkerGlobalCommUsage)
	 */
	@Override
	public void initialize(GraphState graphState,
			WorkerClientRequestProcessor<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> workerClientRequestProcessor,
			CentralizedServiceWorker<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> serviceWorker,
			WorkerGlobalCommUsage workerGlobalCommUsage) {
		super.initialize(graphState, workerClientRequestProcessor, serviceWorker, workerGlobalCommUsage);

		loesToDiscover = (short) ((IntWritable)getAggregatedValue(MSTPathfinderMasterCompute.loesToDiscoverAggregator)).get();
		loesToRemove = loesToDiscover == PathfinderEdgeType.INTERFRAGMENT_EDGE ? 
				PathfinderEdgeType.INTERFRAGMENT_EDGE : PathfinderEdgeType.UNASSIGNED;
	}




	/**
	 * @author spark
	 *
	 */
	public static class LOEDiscoveryTEST  extends LOEDiscoveryComputation{


		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);

			PathfinderVertexType vertexValue = vertex.getValue();

			vertexValue.resetLOE();
			Toolbox.disarmPathfinderCandidates(vertex, loesToRemove);

			if(vertexValue.hasLOEsDepleted() || !vertexValue.boruvkaStatus())
				return;

			if(vertex.getNumEdges() == 1){
				PathfinderVertexID neighbor = vertex.getEdges().iterator().next().getTargetVertexId().copy();
				Toolbox.setEdgeAsBranch(vertex, neighbor);
				sendMessage(neighbor, new ControlledGHSMessage(vertex.getId().copy(), ControlledGHSMessage.FORCE_ACCEPT));
				vertexValue.loesDepleted();
				return;
			}

			//			PathfinderVertexID selectedNeighbor = null;
			Stack<PathfinderVertexID> loes = null;

			//Check for unassigned
			loes = Toolbox.getLOEsForVertex(vertex, loesToDiscover);

			if(loes == null){
				log.info("depleted loes!");
				vertexValue.loesDepleted();
				if(loesToDiscover == PathfinderEdgeType.INTERFRAGMENT_EDGE)
					vertexValue.deactivateForBoruvka();
				return;
			}

			log.info("available loes " + loes.size());

			for(PathfinderVertexID candidate : loes){ //on all edges with same weight a test message is sent
				log.info("sending test message to " + candidate.get());
				sendMessage(candidate, 
						new ControlledGHSMessage(vertex.getId(), 
								vertexValue.getFragmentIdentity().copy(), 
								ControlledGHSMessage.TEST_MESSAGE));
			}
		}

	}

	public static class LOEDiscoveryTEST_REPLY extends LOEDiscoveryComputation{

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
						removeEdgesRequest(vertexId, currentSenderID);
					}
				}else if(currentMessageCode == ControlledGHSMessage.FORCE_ACCEPT){
					Toolbox.setEdgeAsBranch(vertex, currentMessage.getSenderID());
				}
			}
		}		
	}

	public static class LOEDiscoveryREPORT_GENERATION extends LOEDiscoveryComputation{

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
					removeEdgesRequest(vertexId, senderID); 
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

	public static class LOEDiscoveryREPORT_DELIVERY extends LOEDiscoveryComputation {

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
			if(vertexValue.getLOE() != Double.MAX_VALUE && vertexValue.getLOE() <= minLOE){
				//				Toolbox.armPathfinderCandidates(vertex);
				minLOE = vertexValue.getLOE();
				minLOEDestination = vertexValue.getLoeDestination();

				log.info("Root wins " + minLOE + " " + minLOEDestination);
				//				vertex.getEdgeValue(minLOEDestination).setAsBranchEdge();
				//				log.info("Connected " + minLOEDestination);
			}else{
				Toolbox.disarmPathfinderCandidates(vertex, loesToRemove);
				vertexValue.updateLOE(minLOE);
				vertexValue.setLoeDestination(minLOEDestination);
			}
			if(minLOE != Double.MAX_VALUE){
				log.info("sending connect message to " + minLOEDestination + " value" + minLOE);
				sendMessage(minLOEDestination, new ControlledGHSMessage(vertexId, vertexValue.getFragmentIdentity(), ControlledGHSMessage.CONNECT_MESSAGE));
				aggregate(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, new BooleanWritable(false));

				Collection<PathfinderVertexID> branches = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH);

				if(branches != null)
					for(PathfinderVertexID e : branches)
						if(!e.equals(minLOEDestination))
							sendMessage(e, new ControlledGHSMessage(vertexId, ControlledGHSMessage.REFUSE_MESSAGE));
			}
		}		
	}
}


