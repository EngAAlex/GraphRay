/**
 * 
 */
package unipg.pathfinder.ghs.computations;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
import unipg.pathfinder.common.writables.SetWritable;
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

	public static class LOEDiscoveryCleanup extends LOEDiscoveryComputation{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			vertex.getValue().resetLOE();
			vertex.getValue().resetLOEStack();
			//			log.info("Rewverting candidates to " + PathfinderEdgeType.CODE_STRINGS[loesToRemove]);
			Toolbox.disarmPathfinderCandidates(vertex, loesToRemove);
		}

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

			//			vertexValue.resetLOE();
			//			Toolbox.disarmPathfinderCandidates(vertex, loesToRemove);

			if(vertexValue.hasLOEsDepleted() || !vertexValue.boruvkaStatus())
				return;

			if(vertex.getNumEdges() == 1){
				PathfinderVertexID neighbor = vertex.getEdges().iterator().next().getTargetVertexId().copy();
				if(!vertex.getEdgeValue(neighbor).isBranch()){
					Toolbox.setEdgeAsBranch(vertex, neighbor);
					sendMessage(neighbor, new ControlledGHSMessage(vertex.getId().copy(), ControlledGHSMessage.FORCE_ACCEPT));
					vertexValue.loesDepleted();
				}
				return;
			}

			//			PathfinderVertexID selectedNeighbor = null;
			Stack<PathfinderVertexID> loes = null;

			//Check for unassigned
			loes = Toolbox.getLOEsForVertex(vertex, loesToDiscover);

			if(loes == null){
				log.info("depleted loes!");
				vertexValue.loesDepleted();
				//				if(loesToDiscover == PathfinderEdgeType.INTERFRAGMENT_EDGE)
				//					vertexValue.deactivateForBoruvka();
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
						log.info("refused fragment from " + currentSenderID + " removing edge from " + vertex.getId() + " to " + currentSenderID);
						//						getContext().getCounter(MSTPathfinderMasterCompute.counterGroup, MSTPathfinderMasterCompute.prunedCounter).increment(1);
						sendMessage(currentSenderID.copy(), 
								new ControlledGHSMessage(vertexId.copy(), ControlledGHSMessage.REFUSE_MESSAGE));
						removeEdgesRequest(vertexId, currentSenderID);
					}
				}else if(currentMessageCode == ControlledGHSMessage.FORCE_ACCEPT){
					log.info("Force-accepting vertex " + currentSenderID);
					Toolbox.setEdgeAsBranch(vertex, currentSenderID);
					sendMessage(currentSenderID, new ControlledGHSMessage(vertexId, ControlledGHSMessage.ROOT_UPDATE));
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
//			if(vertexValue.hasLOEsDepleted()/* && !vertexValue.isRoot()*/)/*{*/
//				sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertex.getId(), ControlledGHSMessage.LOEs_DEPLETED));
//				return;
//			}
			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			//			HashMap<PathfinderVertexID, Integer> fragmentFrequencyMap = new HashMap<PathfinderVertexID, Integer>();
			//			HashMap<PathfinderVertexID, Stack<PathfinderVertexID>> senderFragmentMap = new HashMap<PathfinderVertexID, Stack<PathfinderVertexID>>();

			MapWritable senderFragmentMap = new MapWritable();

			while(msgs.hasNext()){
				ControlledGHSMessage currentMessage = msgs.next();
				PathfinderVertexID senderID = currentMessage.getSenderID().copy();
				PathfinderVertexID remoteFragment = currentMessage.getFragmentID().copy();
				short currentMessageCode = currentMessage.getStatus();
				switch(currentMessageCode){
				case ControlledGHSMessage.REFUSE_MESSAGE:
					log.info("Received a refuse message from " + senderID + " removing from " + vertex.getId() + " to " + senderID);
					removeEdgesRequest(vertexId, senderID); 
					break;						
				case ControlledGHSMessage.ACCEPT_MESSAGE:
					if(vertex.getEdgeValue(senderID).isBranch())
						continue;
					if(!senderFragmentMap.containsKey(remoteFragment))
						senderFragmentMap.put(remoteFragment, new SetWritable<PathfinderVertexID>());
					((SetWritable<PathfinderVertexID>)(senderFragmentMap.get(remoteFragment))).add(senderID.copy());
					Toolbox.setEdgeAsPathfinderCandidate(vertex, senderID);
					log.info("Received a accept message from " + senderID);
					break;
				case ControlledGHSMessage.ROOT_UPDATE:
					vertexValue.setFragmentIdentity(remoteFragment);
					log.info("Updated root after force-accept");
					return;
					//break;
				}
			}

			if(senderFragmentMap.size() == 0)
				return;			

			vertexValue.setLOEStack(senderFragmentMap);

			if(!vertexValue.isRoot()){
				for(Writable w : senderFragmentMap.keySet()){
					PathfinderVertexID pfid = (PathfinderVertexID) w;
					log.info("Reporting to root " + vertexValue.getFragmentIdentity());
					sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertexId, pfid.copy(), vertexValue.getLOE(), ControlledGHSMessage.REPORT_MESSAGE));
				}
			}

		}	
	}

	public static class LOEDiscoveryREPORT_DELIVERY extends LOEDiscoveryComputation {

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();

			if(!vertexValue.isRoot()) //only roots will react now, and I'm sure only them will have messages incoming
				return;
			
			PathfinderVertexID vertexId = vertex.getId();
			double minLOE = Double.MAX_VALUE;
			MapWritable remoteFragmentsStack = new MapWritable();

//			if(vertexValue.isStackEmpty()){

				Iterator<ControlledGHSMessage> msgs = messages.iterator();
				//			PathfinderVertexID minLOEDestination = null;

				double myLOE = vertexValue.getLOE();
				while(msgs.hasNext()){
					ControlledGHSMessage currentMessage = msgs.next();
//					short currentStatus = currentMessage.getStatus();
//					if(currentStatus == ControlledGHSMessage.LOEs_DEPLETED){
//						continue; 
//					}
					PathfinderVertexID currentSenderID = currentMessage.getSenderID();
					PathfinderVertexID currentFragmentID = currentMessage.getFragmentID();				
					double currentValue = currentMessage.get();
					if(currentValue <= minLOE){
						if(currentValue < minLOE){
							minLOE = currentValue;
							remoteFragmentsStack.clear();	
							log.info("Clear MAS; new candidate fragment " + currentFragmentID + " minloe " + minLOE);
						}
						if(!remoteFragmentsStack.containsKey(currentFragmentID))
							remoteFragmentsStack.put(currentFragmentID.copy(), new SetWritable<PathfinderVertexID>());
						((SetWritable<PathfinderVertexID>)remoteFragmentsStack.get(currentFragmentID.copy())).add(currentSenderID.copy());
						log.info("Adding to MAS; candidate fragment " + currentFragmentID + " sender " + currentSenderID);
						//						vertexValue.addToFragmentStack(currentFragmentID.copy(), currentSenderID.copy());
						//						multipleAcceptanceSet.add(currentSenderID.copy());
					}
				}

				if((!vertexValue.hasLOEsDepleted() && myLOE != Double.MAX_VALUE) && myLOE <= minLOE){
					if(myLOE < minLOE){
						remoteFragmentsStack.clear();
						minLOE = myLOE;
						log.info("Root wins");
					}else
						log.info("Root draws");					
				}else{
					Toolbox.disarmPathfinderCandidates(vertex, loesToRemove);
					vertexValue.resetLOE();
					vertexValue.resetLOEStack();
					log.info("Deleting my data");
				}
			

			log.info("minloe " + (minLOE == Double.MAX_VALUE ? "MAX" : minLOE));
				
			if(minLOE != Double.MAX_VALUE /*|| !vertexValue.isStackEmpty()*/){
				for(Writable k : vertexValue.getActiveFragments()){
					log.info("sending local TEST connect message to fragment " + k);
					sendMessageToMultipleEdges(							
							((SetWritable<PathfinderVertexID>)vertexValue.getRecipientsForFragment(((PathfinderVertexID)k))).iterator(), 
							new ControlledGHSMessage(vertexId, vertexValue.getFragmentIdentity(), ControlledGHSMessage.CONNECT_TEST)); //CONNECT_MESSAGE
					aggregate(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, new BooleanWritable(false));					
				}
				for(Writable k : remoteFragmentsStack.keySet()){
					log.info("sending remote TEST connect message to fragment " + k);					
					sendMessageToMultipleEdges(((SetWritable<PathfinderVertexID>)remoteFragmentsStack.get(k)).iterator(), 
							new ControlledGHSMessage(vertexId, ((PathfinderVertexID)k).copy(), ControlledGHSMessage.CONNECT_TEST)); //CONNECT_MESSAGE
					aggregate(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, new BooleanWritable(false));					
				}
			}
		}		
	}
}



