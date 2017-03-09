/**
 * 
 */
package com.graphray.ghs.computations;

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

import com.graphray.GraphRayComputation;
import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.messagetypes.ControlledGHSMessage;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;
import com.graphray.common.writables.SetWritable;
import com.graphray.masters.GraphRayMasterCompute;
import com.graphray.utils.Toolbox;



public abstract class LOEDiscoveryComputation extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

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
			Toolbox.disarmPathfinderCandidates(vertex);
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
			loes = Toolbox.getLOEsForVertex(vertex);

			if(loes == null){
				if(isLogEnabled)
					log.info("depleted loes!");
				vertexValue.loesDepleted();
				//				if(loesToDiscover == PathfinderEdgeType.INTERFRAGMENT_EDGE)
				//					vertexValue.deactivateForBoruvka();
				return;
			}
			
			if(isLogEnabled)
				log.info("available loes " + loes.size());

			for(PathfinderVertexID candidate : loes){ //on all edges with same weight a test message is sent
				if(isLogEnabled)
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
				if(isLogEnabled)
					log.info("received fragment " + currentFragment);
				if(currentMessageCode == ControlledGHSMessage.TEST_MESSAGE){ 
					if(!myFragment.equals(currentFragment)){ //connection accepted
						if(isLogEnabled)
							log.info("accepted fragment");
						sendMessage(currentSenderID.copy(), 
								new ControlledGHSMessage(vertexId.copy(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ACCEPT_MESSAGE)); //all accept edges will receive also the 
					}else{ //connection refused																										//neighboring fragment ID
						if(isLogEnabled)
							log.info("refused fragment from " + currentSenderID + " removing edge from " + vertex.getId() + " to " + currentSenderID);
						//						getContext().getCounter(MSTPathfinderMasterCompute.counterGroup, MSTPathfinderMasterCompute.prunedCounter).increment(1);
						sendMessage(currentSenderID.copy(), 
								new ControlledGHSMessage(vertexId.copy(), ControlledGHSMessage.REFUSE_MESSAGE));
						removeEdgesRequest(vertexId, currentSenderID);
					}
				}else if(currentMessageCode == ControlledGHSMessage.FORCE_ACCEPT){
					if(isLogEnabled)
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
					if(isLogEnabled)
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
					if(isLogEnabled)
						log.info("Received a accept message from " + senderID);
					break;
				case ControlledGHSMessage.ROOT_UPDATE:
					vertexValue.setFragmentIdentity(remoteFragment);
					if(isLogEnabled)
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
					if(isLogEnabled)
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
					PathfinderVertexID currentSenderID = currentMessage.getSenderID();
					PathfinderVertexID currentFragmentID = currentMessage.getFragmentID();				
					double currentValue = currentMessage.get();
					if(currentValue <= minLOE){
						if(currentValue < minLOE){
							minLOE = currentValue;
							remoteFragmentsStack.clear();	
							if(isLogEnabled)
								log.info("Clear MAS; new candidate fragment " + currentFragmentID + " minloe " + minLOE);
						}
						if(!remoteFragmentsStack.containsKey(currentFragmentID))
							remoteFragmentsStack.put(currentFragmentID.copy(), new SetWritable<PathfinderVertexID>());
						((SetWritable<PathfinderVertexID>)remoteFragmentsStack.get(currentFragmentID.copy())).add(currentSenderID.copy());
						if(isLogEnabled)
							log.info("Adding to MAS; candidate fragment " + currentFragmentID + " sender " + currentSenderID);
					}
				}

				if((!vertexValue.hasLOEsDepleted() && myLOE != Double.MAX_VALUE) && myLOE <= minLOE){
					if(myLOE < minLOE){
						remoteFragmentsStack.clear();
						minLOE = myLOE;
						if(isLogEnabled)
							log.info("Root wins");
					}else
						if(isLogEnabled)
							log.info("Root draws");					
				}else{
					Toolbox.disarmPathfinderCandidates(vertex);
					vertexValue.resetLOE();
					vertexValue.resetLOEStack();
					if(isLogEnabled)
						log.info("Deleting my data");
				}
			
				if(isLogEnabled)
					log.info("minloe " + (minLOE == Double.MAX_VALUE ? "MAX" : minLOE));
				
			if(minLOE != Double.MAX_VALUE /*|| !vertexValue.isStackEmpty()*/){
				for(Writable k : vertexValue.getActiveFragments()){
					if(isLogEnabled)
						log.info("sending local TEST connect message to fragment " + k);
					sendMessageToMultipleEdges(							
							((SetWritable<PathfinderVertexID>)vertexValue.getRecipientsForFragment(((PathfinderVertexID)k))).iterator(), 
							new ControlledGHSMessage(vertexId, vertexValue.getFragmentIdentity(), ControlledGHSMessage.CONNECT_TEST)); //CONNECT_MESSAGE
				}
				for(Writable k : remoteFragmentsStack.keySet()){
					if(isLogEnabled)
						log.info("sending remote TEST connect message to fragment " + k);					
					sendMessageToMultipleEdges(((SetWritable<PathfinderVertexID>)remoteFragmentsStack.get(k)).iterator(), 
							new ControlledGHSMessage(vertexId, ((PathfinderVertexID)k).copy(), ControlledGHSMessage.CONNECT_TEST)); //CONNECT_MESSAGE
				}
			}
		}		
	}
}



