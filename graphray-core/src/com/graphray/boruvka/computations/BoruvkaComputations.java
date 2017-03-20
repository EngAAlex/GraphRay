/**
 * 
 */
package com.graphray.boruvka.computations;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import com.graphray.GraphRayComputation;
import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.messagetypes.ConnectedFragmentsSlimMessage;
import com.graphray.common.messagetypes.ControlledGHSMessage;
import com.graphray.common.messagetypes.SlimControlledGHSMessage;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;
import com.graphray.common.writables.SetWritable;
import com.graphray.masters.GraphRayMasterCompute;
import com.graphray.utils.Toolbox;

/**
 * @author spark
 *
 */
public class BoruvkaComputations {

	public static class BoruvkaRootUpdateSetup extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.isRoot() && !vertexValue.boruvkaStatus()){ //the vertex is a deactivated root and must be updated
				PathfinderVertexID myFragment = vertexValue.getFragmentIdentity();
				vertexValue.popSetOutOfStack(myFragment);							
				if(!vertexValue.hasLOEsDepleted()){
					if(isLogEnabled)
						log.info("Adding edge as dummy to " + myFragment);					
					if(vertex.getEdgeValue(myFragment) == null){
						Toolbox.connectWithDummies(this, vertex, myFragment);
					}else if(vertex.getEdgeValue(myFragment).unassigned()){
						Toolbox.updateEdgeValueWithStatus(vertex, PathfinderEdgeType.DUMMY, myFragment);
						Toolbox.updateRemoteEdgeWithStatus(this, vertex.getId(), myFragment, vertex.getEdgeValue(myFragment), PathfinderEdgeType.DUMMY);
					}
					if(isLogEnabled)
						log.info("LOES not depleted");
				}
			}else if(vertexValue.isRoot()){
				if(isLogEnabled)
					log.info("I'm still a root during Boruvka");
				aggregate(GraphRayMasterCompute.boruvkaProcedureCompletedAggregator, new IntWritable(1));
			}
		}
	}

	public static class BoruvkaRootUpdateConfirmationPing extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();		
			if(vertexValue.isRoot() && !vertexValue.boruvkaStatus()){ //the vertex is a deactivated root and must be updated			
				PathfinderVertexID myFragment = vertexValue.getFragmentIdentity();
				if(isLogEnabled)
					log.info("Asking for confirmation to " + myFragment);
				sendMessage(myFragment, new ControlledGHSMessage(vertex.getId(), myFragment, ControlledGHSMessage.ROOT_STATUS));
				vertexValue.setRoot(false);
				vertexValue.reactivateForBoruvka();
			}
		}
	}

	public static class BoruvkaRootUpdateConfirmationReply extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			PathfinderVertexID myFragment = vertexValue.getFragmentIdentity();

			Iterator<ControlledGHSMessage> msgs = messages.iterator();

			HashSet<PathfinderVertexID> verticesToNotify = null;					

			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID msgFragmentID = current.getFragmentID();
				PathfinderVertexID msgSenderID = current.getSenderID();
				if(current.getStatus() == ControlledGHSMessage.ROOT_STATUS){
					vertexValue.popSetOutOfStack(msgFragmentID);
					continue;
				}
				if(!msgFragmentID.equals(myFragment)){
					if(verticesToNotify == null)
						verticesToNotify = new HashSet<PathfinderVertexID>();
					if(isLogEnabled)
						log.info("Notifying " + msgSenderID + " of updated root " + myFragment);
					verticesToNotify.add(msgSenderID.copy());
				}else
					if(isLogEnabled)
						log.info("Fragment match -- no need for update");
			}

			if(verticesToNotify != null)
				sendMessageToMultipleEdges(verticesToNotify.iterator(), new ControlledGHSMessage(vertex.getId(), myFragment, ControlledGHSMessage.ROOT_UPDATE));
		}
	}

	public static class BoruvkaRootUpdateConfirmationCompletion extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			PathfinderVertexID myFragment = vertexValue.getFragmentIdentity();

			Iterator<ControlledGHSMessage> msgs = messages.iterator();

			if(msgs.hasNext())
				while(msgs.hasNext()){
					ControlledGHSMessage current = msgs.next();
					if(!current.getFragmentID().equals(myFragment)){
						if(isLogEnabled)
							log.info("Updating with correct fragment " + current.getFragmentID());
						Toolbox.removeExistingDummies(this, vertex, myFragment);
						myFragment = current.getFragmentID().copy();						
						vertexValue.setFragmentIdentity(myFragment);
					}
				}
			else
				if(isLogEnabled)
					log.info("Fragment already " + myFragment);

			Collection<PathfinderVertexID> verticesToNotify = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH, PathfinderEdgeType.PATHFINDER, PathfinderEdgeType.DUMMY);

			if(verticesToNotify != null)
				sendMessageToMultipleEdges(verticesToNotify.iterator(), new ControlledGHSMessage(vertex.getId(), myFragment, ControlledGHSMessage.ROOT_UPDATE));
		}
	}

	public static class BoruvkaRootUpdateCompletion extends GraphRayComputation<ControlledGHSMessage, SlimControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(!vertexValue.boruvkaStatus() || vertex.getValue().isRoot()){
				vertexValue.getReadyForNextRound();
				return;
			}
			super.compute(vertex, messages);

			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			if(!msgs.hasNext())
				return;
			PathfinderVertexID newFragmentID = null;
			//			PathfinderVertexID oldFragment = vertexValue.getOldFragmentID();			
			PathfinderVertexID myFragment = vertexValue.getFragmentIdentity().copy();
			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID currentFragment = current.getFragmentID().copy();
				if(!current.getSenderID().equals(myFragment))
					continue;
				if(isLogEnabled)
					log.info("Received " + currentFragment + " from " + current.getSenderID());
				if(myFragment.equals(currentFragment)){
					if(isLogEnabled)
						log.info("Fragment already  " + currentFragment);	
					continue;
					//					return;
				}else if(currentFragment.equals(vertex.getId())){
					if(isLogEnabled)
						log.info("discarding self id -- received from " + current.getSenderID());
					//				removeEdgesRequest(vertex.getId(), vertex.getId());
					//					return;
					continue;
				}
				if(myFragment.get() < currentFragment.get() && (newFragmentID == null || newFragmentID.get() < currentFragment.get())){
					if(isLogEnabled)
						log.info("Updating with new  fragment");
					newFragmentID = currentFragment;
				}
			}

			if(newFragmentID != null){
				if(isLogEnabled)				
					log.info("Updating with new fragment " + newFragmentID);
//				vertexValue.popSetOutOfStack(newFragmentID);
				//				vertexValue.setLastConnectedFragment(myFragment);
				//				oldFragment = myFragment;
//				vertexValue.setOldFragmentID(myFragment);
//				Iterable<PathfinderVertexID> dummyEdges = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.DUMMY);
//				if(dummyEdges != null){
//					Iterator<PathfinderVertexID> dummyEdgesIt = dummyEdges.iterator();
//					while(dummyEdgesIt.hasNext()){
//						PathfinderVertexID currentDummy = dummyEdgesIt.next();			//old pair of dummies are removed
//						//						log.info("Removing existing dummy " + currentDummy);
//						Toolbox.removeExistingDummies(this, vertex, currentDummy.copy());
//					}
//				}

				vertexValue.setFragmentIdentity(newFragmentID);

				if(vertex.getEdgeValue(newFragmentID) == null){
					Toolbox.connectWithDummies(this, vertex, newFragmentID);
					//					log.info("Added new dummies to " + newFragmentID);
				}else if(vertex.getEdgeValue(newFragmentID).unassigned()){
					Toolbox.updateEdgeValueWithStatus(vertex, PathfinderEdgeType.DUMMY, newFragmentID);
					Toolbox.updateRemoteEdgeWithStatus(this, vertex.getId(), newFragmentID, vertex.getEdgeValue(newFragmentID), PathfinderEdgeType.DUMMY);
				}
				
			}
//			else
//				vertexValue.popSetOutOfStack(myFragment);
			
			Collection<PathfinderVertexID> destinations = Toolbox.getUnassignedEdgesByWeight(vertex, vertexValue.getLOE());
			if(destinations != null)
				sendMessageToMultipleEdges(destinations.iterator(), new SlimControlledGHSMessage(vertex.getId(), vertexValue.getFragmentIdentity()));

		}
	}
	
	public static class BoruvkaFindRemainingPathfinders extends GraphRayComputation<SlimControlledGHSMessage, SlimControlledGHSMessage>{
		
		/* (non-Javadoc)
		 * @see com.graphray.GraphRayComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<SlimControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			for(SlimControlledGHSMessage msg : messages){
				if(msg.getFragmentID().equals(vertexValue.getFragmentIdentity()))
					Toolbox.updateEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER, msg.getSenderID().copy());//(vertex, stackToUpdate);	
			}
			vertexValue.getReadyForNextRound();
		}
		
	}


//	public static class BoruvkaUpdateConnectedFragmentsPing extends GraphRayComputation<ControlledGHSMessage, ConnectedFragmentsSlimMessage>{
//
//		/* (non-Javadoc)
//		 * @see com.graphray.GraphRayComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
//		 */
//		@Override
//		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
//				Iterable<ControlledGHSMessage> messages) throws IOException {
//			super.compute(vertex, messages);
//			PathfinderVertexType vertexValue = vertex.getValue();
//			SetWritable<PathfinderVertexID> connectedFragments = vertexValue.getLastConnectedFragments();
//
//			if(!vertexValue.isRoot() || (vertexValue.isRoot() && connectedFragments.size() == 0))
//				return;
//			Collection<PathfinderVertexID> destinations = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.DUMMY, PathfinderEdgeType.BRANCH, PathfinderEdgeType.PATHFINDER);			
//			for(PathfinderVertexID connectedFragment : connectedFragments){
//				SetWritable<PathfinderVertexID> currentStack = vertexValue.popSetOutOfStack(connectedFragment);
//				//				if(connectedFragment != null){
//				//					if(destinations != null){
//				if(isLogEnabled)
//					log.info("Informing cluster of connected fragment " + connectedFragment);
//				//					}
//				//				}
//			}
//
//			if(isLogEnabled)
//				log.info("Size of connected fragments " + connectedFragments.size());
//
//			Collection<PathfinderVertexID> inactiveBoundary = vertexValue.getActiveBoundary();
//
//			if(destinations != null){
//				if(inactiveBoundary != null)			
//					destinations.removeAll(inactiveBoundary);
//				if(destinations.size() > 0){
//					ConnectedFragmentsSlimMessage toSend = new ConnectedFragmentsSlimMessage(vertex.getId(), vertexValue.getFragmentLoeValue());
//					toSend.setSetOfFragments(connectedFragments);
//					//				for(PathfinderVertexID destination : destinations)
//					//					sendMessage(destination, toSend.copy());
//					sendMessageToMultipleEdges(destinations.iterator(), toSend);
//				}
//			}
//		}		
//	}

//	public static class BoruvkaUpdateConnectedFragmentsReply extends GraphRayComputation<ConnectedFragmentsSlimMessage, ControlledGHSMessage>{
//
//		/* (non-Javadoc)
//		 * @see com.graphray.GraphRayComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
//		 */
//		@Override
//		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
//				Iterable<ConnectedFragmentsSlimMessage> messages) throws IOException {
//			super.compute(vertex, messages);
//			PathfinderVertexType vertexValue = vertex.getValue();
//			SetWritable<PathfinderVertexID> stackToUpdate = null;
//			HashSet<PathfinderVertexID> stackToExclude = null;
//
//			vertexValue.clearBoundary();
//
//			for(ConnectedFragmentsSlimMessage msg : messages){
//				//				PathfinderVertexID msgFragment = msg.getFragmentID();
//				double fragmentLOE = msg.get();
//				Collection<PathfinderVertexID> connectedFragments = msg.getSetOfFragments();
//				if(isLogEnabled)
//					log.info("Received from " + msg.getSenderID() + " stack of size " + connectedFragments.size());
//				if(msg.getSenderID().equals(vertexValue.getFragmentIdentity())){
//					for(PathfinderVertexID msgFragment : connectedFragments){
//						if(isLogEnabled)
//							log.info("Received fragment " + msgFragment);
//						stackToUpdate = vertexValue.popSetOutOfStack(msgFragment);
//						if(stackToUpdate == null)
//							continue;
//
//						if(isLogEnabled)
//							log.info("updating last connected fragment -- removing from stack " + msgFragment);
//
//						if(fragmentLOE == vertexValue.getLOE()){
//							for(PathfinderVertexID e : stackToUpdate)
//								if(vertex.getEdgeValue(e).unassigned()){
//									if(isLogEnabled)
//										log.info("Pathfinder connection " + e);
//									Toolbox.updateEdgeValueWithStatus(vertex, PathfinderEdgeType.PATHFINDER, e);//(vertex, stackToUpdate);	
//									if(stackToExclude == null)
//										stackToExclude = new HashSet<PathfinderVertexID>();
//									stackToExclude.add(e.copy());
//								}
//						}
//					}
//				}
//				//				}
//			}
//
//			PathfinderVertexID oldFragment = vertexValue.getOldFragmentID();
//
//			if(oldFragment != null){
//				vertexValue.popSetOutOfStack(oldFragment);
//				//if(/*vertexValue.isStackEmpty() && */!vertexValue.hasLOEsDepleted()){
//				Collection<PathfinderVertexID> destinations = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.UNASSIGNED);
//				if(destinations != null){
//
//					if(stackToExclude != null)
//						destinations.removeAll(stackToExclude);
//					if(isLogEnabled)
//						log.info("Informing unassigned edges about my new fragment");
//
//					sendMessageToMultipleEdges(destinations.iterator(), new ControlledGHSMessage(vertex.getId(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ROOT_UPDATE));
//					sendMessageToMultipleEdges(destinations.iterator(), new ControlledGHSMessage(vertex.getId(), oldFragment, ControlledGHSMessage.ROOT_STATUS));					
//				}
//
//			}
//		}
//
//	}
//
//	public static class BoruvkaUnassignedEdgesFinalUpdate extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{
//
//		/* (non-Javadoc)
//		 * @see com.graphray.GraphRayComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
//		 */
//		@Override
//		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
//				Iterable<ControlledGHSMessage> messages) throws IOException {
//			super.compute(vertex, messages);
//			PathfinderVertexType vertexValue = vertex.getValue();						
//			if(!vertexValue.isStackEmpty()/*vertexValue.getLOE() != Double.MAX_VALUE*/){
//				double value = vertexValue.getLOE();
//				for(ControlledGHSMessage msg : messages){
//					PathfinderVertexID senderID = msg.getSenderID();
//					PathfinderVertexID msgFragmentID = msg.getFragmentID();
//					if(isLogEnabled)
//						log.info("Receing unassigned " + msg.getStatus() +  " root update from sender " +
//								senderID + " fragment " + msgFragmentID/* + " valye " + vertex.getEdgeValue(senderID).get()*/);
//					if(vertex.getEdgeValue(senderID).get() != value)
//						continue;
//					switch(msg.getStatus()){
//					case ControlledGHSMessage.ROOT_UPDATE:
//						if(!msgFragmentID.equals(vertexValue.getFragmentIdentity())){
//							if(isLogEnabled)
//								log.info("adding " + msg.getSenderID() + " to fragment " + msg.getFragmentID());
//							vertexValue.addVertexToFragment(msg.getSenderID(), msg.getFragmentID());
//						}else{
//							if(isLogEnabled)							
//								log.info("Removing edge, vertices are in same fragment from " + senderID + " to " + vertex.getId());
//							removeEdgesRequest(senderID.copy(), vertex.getId());
//							removeEdgesRequest(vertex.getId(), senderID.copy());
//						}
//						break;
//					case ControlledGHSMessage.ROOT_STATUS:
//						vertexValue.popSetOutOfStack(senderID);
//						//						log.info("removing " + msg.getSenderID() + " fromg fragment " + msg.getFragmentID());					
//						vertexValue.removeVertexFromFragment(msg.getSenderID(), msg.getFragmentID());					
//						break;
//					}
//				}
//			}
//		}
//	}
}
