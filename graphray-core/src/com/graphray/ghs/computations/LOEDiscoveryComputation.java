/**
 * 
 */
package com.graphray.ghs.computations;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Stack;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.graphray.GraphRayComputation;
import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.messagetypes.ControlledGHSMessage;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;
import com.graphray.common.writables.SetWritable;
import com.graphray.utils.Toolbox;



public abstract class LOEDiscoveryComputation extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

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

			if(vertexValue.hasLOEsDepleted()/* || !vertexValue.boruvkaStatus()*/)
				return;

			if(vertex.getNumEdges() == 1){
				PathfinderVertexID neighbor = vertex.getEdges().iterator().next().getTargetVertexId().copy();
				if(!vertex.getEdgeValue(neighbor).isBranch()){
					Toolbox.setEdgeAsBranch(vertex, neighbor);
					sendMessage(neighbor, new ControlledGHSMessage(vertex.getId().copy(), ControlledGHSMessage.FORCE_ACCEPT));
					vertexValue.deactivateForBoruvka();
					vertexValue.setRoot(false);
					vertexValue.loesDepleted();
				}
				return;
			}

			vertexValue.getReadyForNextRound();

			if(!vertexValue.isStackEmpty()){
				if(isLogEnabled)
					log.info("available loes in stack " + vertexValue.stackSize());
				return;
			}else
				if(isLogEnabled)
					log.info("Staack emptied, looking for new loes");

			vertexValue.resetLOE();
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
			super.compute(vertex, messages);
			
			if(vertex.getNumEdges() == 1)
				return;
			
			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexType vertexValue = vertex.getValue();
			PathfinderVertexID myFragment = vertexValue.getFragmentIdentity();
			Iterator<ControlledGHSMessage> msgIterator = messages.iterator();
			//			double myLOE = vertexValue.getLOE();
			while(msgIterator.hasNext()){				
				ControlledGHSMessage currentMessage = msgIterator.next();
				PathfinderVertexID remoteSenderID = currentMessage.getSenderID().copy();
				PathfinderVertexID remoteFragment = currentMessage.getFragmentID().copy();
				short currentMessageCode = currentMessage.getStatus();
				if(isLogEnabled)				
					log.info("received fragment " + remoteFragment);
				if(currentMessageCode == ControlledGHSMessage.TEST_MESSAGE){ 
					if(!myFragment.equals(remoteFragment)){ //connection accepted
						//						if(vertex.getEdgeValue(remoteSenderID).get() <= myLOE){
						if(isLogEnabled)
							log.info("accepted fragment");
						sendMessage(remoteSenderID.copy(), 
								new ControlledGHSMessage(vertexId.copy(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ACCEPT_MESSAGE)); //all accept edges will receive also the
						//						}
					}else{ //connection refused																										//neighboring fragment ID
						if(isLogEnabled)
							log.info("refused fragment "+ remoteFragment + " from " + remoteSenderID + " removing edge from " + vertex.getId() + " to " + remoteSenderID);
						//						getContext().getCounter(MSTPathfinderMasterCompute.counterGroup, MSTPathfinderMasterCompute.prunedCounter).increment(1);
						sendMessage(remoteSenderID.copy(), 
								new ControlledGHSMessage(vertexId.copy(), ControlledGHSMessage.REFUSE_MESSAGE));
						removeEdgesRequest(vertexId, remoteSenderID);
						vertexValue.popSetOutOfStack(remoteFragment);						
					}
				}else if(currentMessageCode == ControlledGHSMessage.FORCE_ACCEPT){
					if(isLogEnabled)
						log.info("Force-accepting vertex " + remoteSenderID);
					Toolbox.setEdgeAsBranch(vertex, remoteSenderID);
					sendMessage(remoteSenderID, new ControlledGHSMessage(vertexId, ControlledGHSMessage.ROOT_UPDATE));
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

			Iterator<ControlledGHSMessage> msgs = messages.iterator();

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
					vertexValue.popSetOutOfStack(remoteFragment);
					break;						
				case ControlledGHSMessage.ACCEPT_MESSAGE:
					if(vertex.getEdgeValue(senderID).isBranch())
						continue;

					vertexValue.addToFragmentStack(remoteFragment, senderID);
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

			if(vertexValue.isStackEmpty())
				return;			

			if(isLogEnabled)
				for(Writable w : vertexValue.getActiveFragments()){
					PathfinderVertexID pfid = (PathfinderVertexID) w;
					log.info("Fragment " + pfid);
					log.info("Elements " + ((SetWritable<PathfinderVertexID>)vertexValue.getRecipientsForFragment(pfid)).toString());

				}

			if(!vertexValue.isRoot()){
				for(Writable w : vertexValue.getActiveFragments()){
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
				//					short currentStatus = currentMessage.getStatus();
				//					if(currentStatus == ControlledGHSMessage.LOEs_DEPLETED){
				//						continue; 
				//					}
				PathfinderVertexID currentSenderID = currentMessage.getSenderID();
				PathfinderVertexID currentFragmentID = currentMessage.getFragmentID();		
				
				vertexValue.addToBoundary(currentSenderID.copy());
				
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
					//						vertexValue.addToFragmentStack(currentFragmentID.copy(), currentSenderID.copy());
					//						multipleAcceptanceSet.add(currentSenderID.copy());
				}
			}

			if((!vertexValue.hasLOEsDepleted() && myLOE != Double.MAX_VALUE) && myLOE <= minLOE){
				if(myLOE < minLOE){
					remoteFragmentsStack.clear();
					minLOE = myLOE;
//					log.info("Root wins");
				}//else
//					log.info("Root draws");					
			}


			if(minLOE != Double.MAX_VALUE /*|| !vertexValue.isStackEmpty()*/){
				if(myLOE <= minLOE)
					for(Writable k : vertexValue.getActiveFragments()){
						if(isLogEnabled)
							log.info("sending local TEST connect message to fragment " + k);
						sendMessageToMultipleEdges(							
								((SetWritable<PathfinderVertexID>)vertexValue.getRecipientsForFragment(((PathfinderVertexID)k))).iterator(), 
								new ControlledGHSMessage(vertexId, vertexValue.getFragmentIdentity(), minLOE, ControlledGHSMessage.CONNECT_TEST)); //CONNECT_MESSAGE
						//					aggregate(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, new BooleanWritable(false));					
					}
				for(Writable k : remoteFragmentsStack.keySet()){
					if(isLogEnabled)					
						log.info("sending remote TEST connect message to fragment " + k);					
					sendMessageToMultipleEdges(((SetWritable<PathfinderVertexID>)remoteFragmentsStack.get(k)).iterator(), 
							new ControlledGHSMessage(vertexId, ((PathfinderVertexID)k).copy(), minLOE, ControlledGHSMessage.CONNECT_TEST)); //CONNECT_MESSAGE
					//					aggregate(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator, new BooleanWritable(false));					
				}
			}
			
			Collection<PathfinderVertexID> destinations = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.DUMMY, PathfinderEdgeType.BRANCH, PathfinderEdgeType.PATHFINDER);		
			if(destinations == null)
				return;
			destinations.removeAll(vertexValue.getActiveBoundary());
			vertexValue.replaceBoundarySet(destinations);
		}		
	}
}



