/**
 * 
 */
package com.graphray.ghs.computations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.graphray.GraphRayComputation;
import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.messagetypes.ControlledGHSMessage;
import com.graphray.common.messagetypes.SlimControlledGHSMessage;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;
import com.graphray.common.writables.SetWritable;
import com.graphray.utils.Toolbox;



public abstract class LOEDiscoveryComputation extends GraphRayComputation<SlimControlledGHSMessage, SlimControlledGHSMessage>{

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
				Iterable<SlimControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);

			PathfinderVertexType vertexValue = vertex.getValue();

			if(vertexValue.hasLOEsDepleted()/* || !vertexValue.boruvkaStatus()*/)
				return;

			if(vertex.getNumEdges() == 1){
				Edge<PathfinderVertexID, PathfinderEdgeType> neighbor = vertex.getEdges().iterator().next();
				if(!neighbor.getValue().isBranch()){
					Toolbox.setEdgeAsBranch(vertex, neighbor.getTargetVertexId().copy());
					Toolbox.setRemoteEdgeAsBranch(this, vertex.getId(), neighbor.getValue(), neighbor.getTargetVertexId().copy());
					//					sendMessage(neighbor, new SlimControlledGHSMessage(vertex.getId().copy(), new PathfinderVertexID(-1)));
					vertexValue.deactivateForBoruvka();
					vertexValue.setRoot(false);
					vertexValue.loesDepleted();
				}
				return;
			}

			//			vertexValue.getReadyForNextRound();

			//			if(!vertexValue.isStackEmpty()){
			//				if(isLogEnabled)
			//					log.info("available loes in stack " + vertexValue.stackSize());
			//				return;
			//			}else
			//				if(isLogEnabled)
			//					log.info("Staack emptied, looking for new loes");

			//			vertexValue.resetLOE();
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

			vertexValue.setHowManyLoes(loes.size());

			for(PathfinderVertexID candidate : loes){ //on all edges with same weight a test message is sent
				if(isLogEnabled)
					log.info("sending test message to " + candidate.get());
				sendMessage(candidate, 
						new SlimControlledGHSMessage(vertex.getId(), 
								vertexValue.getFragmentIdentity().copy()));
			}
		}	
	}

	public static class LOEDiscoveryTEST_REPLY extends GraphRayComputation<SlimControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<SlimControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);

			if(vertex.getNumEdges() == 1)
				return;

			double currentLOEValue = vertex.getValue().getLOE();
			int howManyCandidates = vertex.getValue().getHowManyLoes();

			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexType vertexValue = vertex.getValue();
			PathfinderVertexID myFragment = vertexValue.getFragmentIdentity();
			Iterator<SlimControlledGHSMessage> msgIterator = messages.iterator();
			//			double myLOE = vertexValue.getLOE();
			//			double myLOE = vertexValue.getLOE();
			while(msgIterator.hasNext()){ //connection refused	
				SlimControlledGHSMessage currentMessage = msgIterator.next();
				PathfinderVertexID remoteSenderID = currentMessage.getSenderID().copy();
				PathfinderVertexID remoteFragment = currentMessage.getFragmentID().copy();//neighboring fragment ID

				if(myFragment.equals(remoteFragment)){ //connection accepted
					
					if(isLogEnabled)
						log.info("refused fragment "+ remoteFragment + " from " + remoteSenderID + " removing edge from " + vertex.getId() + " to " + remoteSenderID);
					//						getContext().getCounter(MSTPathfinderMasterCompute.counterGroup, MSTPathfinderMasterCompute.prunedCounter).increment(1);
					sendMessage(remoteSenderID.copy(), 
							new ControlledGHSMessage(vertexId.copy(), ControlledGHSMessage.REFUSE_MESSAGE));
					removeEdgesRequest(vertexId, remoteSenderID);
					
					if(vertex.getEdgeValue(remoteSenderID).get() == currentLOEValue)
						howManyCandidates--;
				}
				//						vertexValue.popSetOutOfStack(remoteFragment);						
			}

			msgIterator = messages.iterator();
			
			double alternativeMinLOEValue = Double.MAX_VALUE;
			HashMap<PathfinderVertexID, PathfinderVertexID> alternatives = null;
			if(howManyCandidates == 0)
				alternatives = new HashMap<PathfinderVertexID, PathfinderVertexID>();

			while(msgIterator.hasNext()){				
				SlimControlledGHSMessage currentMessage = msgIterator.next();
				PathfinderVertexID remoteSenderID = currentMessage.getSenderID().copy();
				PathfinderVertexID remoteFragment = currentMessage.getFragmentID().copy();
				
				//				short currentMessageCode = currentMessage.getStatus();
				//				if(remoteFragment.get() != -1){// ControlledGHSMessage.TEST_MESSAGE){ 
				if(!myFragment.equals(remoteFragment)){ //connection accepted
					if(isLogEnabled)
						log.info("accepted fragment " + remoteFragment);
					//						if(vertex.getEdgeValue(remoteSenderID).get() <= myLOE){
					sendMessage(remoteSenderID.copy(), 
							new ControlledGHSMessage(vertexId.copy(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ACCEPT_MESSAGE)); //all accept edges will receive also the
					//						}
					if(howManyCandidates == 0){
						double remoteLOEValue = vertex.getEdgeValue(remoteSenderID).get();
						if(remoteLOEValue > currentLOEValue && remoteLOEValue < alternativeMinLOEValue){
							alternatives.clear();
							alternatives.put(remoteSenderID.copy(), remoteFragment.copy());
						}else if(remoteLOEValue == alternativeMinLOEValue)
							alternatives.put(remoteSenderID.copy(), remoteFragment.copy());							
					}
				}			
			}
			
			if(howManyCandidates == 0){
				Iterator<Entry<PathfinderVertexID, PathfinderVertexID>> it = alternatives.entrySet().iterator();
				
				while(it.hasNext()){
					Entry<PathfinderVertexID, PathfinderVertexID> current = it.next();
					vertex.getValue().addToFragmentStack(current.getValue(), current.getKey());					
				}
			}
			//				else{
			//					if(isLogEnabled)
			//						log.info("Force-accepting vertex " + remoteSenderID);
			//					Toolbox.setEdgeAsBranch(vertex, remoteSenderID);
			//					sendMessage(remoteSenderID, new ControlledGHSMessage(vertexId, ControlledGHSMessage.ROOT_UPDATE));
			//				}
			//			}
		}		
	}

	public static class LOEDiscoveryREPORT_GENERATION extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

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
					//					vertexValue.popSetOutOfStack(remoteFragment);
					break;						
				case ControlledGHSMessage.ACCEPT_MESSAGE:
					if(vertex.getEdgeValue(senderID).isBranch())
						continue;

					vertexValue.addToFragmentStack(remoteFragment, senderID);
					if(isLogEnabled)
						log.info("Received a accept message from " + senderID);
					break;
					//				case ControlledGHSMessage.ROOT_UPDATE:
					//					vertexValue.setFragmentIdentity(remoteFragment);
					//					if(isLogEnabled)
					//						log.info("Updated root after force-accept");
					//					return;
					//break;
				}
			}

			if(vertexValue.isStackEmpty())
				return;			

			if(isLogEnabled)
				for(Writable w : vertexValue.getActiveFragments()){
					PathfinderVertexID pfid = (PathfinderVertexID) w;
					log.info("Fragment " + pfid);
					log.info("Elements " + ((SetWritable<PathfinderVertexID>)vertexValue.peekSetOutOfStack(pfid)).toString());

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

	public static class LOEDiscoveryREPORT_DELIVERY extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage> {

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

				//				vertexValue.addToBoundary(currentSenderID.copy());

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
								((SetWritable<PathfinderVertexID>)vertexValue.peekSetOutOfStack(((PathfinderVertexID)k))).iterator(), 
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

			//			Collection<PathfinderVertexID> destinations = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.DUMMY, PathfinderEdgeType.BRANCH, PathfinderEdgeType.PATHFINDER);		
			//			if(destinations == null)
			//				return;
			//			destinations.removeAll(vertexValue.getActiveBoundary());
			//			vertexValue.replaceBoundarySet(destinations);
		}		
	}
}



