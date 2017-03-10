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
import com.graphray.common.messagetypes.ControlledGHSMessage;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;
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
				//				else
				//					vertexValue.setRoot(false); //the vertex will remain silent from now on				
			}else if(vertexValue.isRoot()){
				if(isLogEnabled)
					log.info("I'm still a root during Boruvka");
				aggregate(GraphRayMasterCompute.boruvkaProcedureCompletedAggregator, new IntWritable(1));
				PathfinderVertexID connectedFragment = vertexValue.getLastConnectedFragment();
				vertexValue.popSetOutOfStack(connectedFragment);			
				if(connectedFragment != null){
//					log.info("Informing cluster of connected fragment " + connectedFragment);
					Collection<PathfinderVertexID> destinations = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.DUMMY, PathfinderEdgeType.BRANCH, PathfinderEdgeType.PATHFINDER);
					if(destinations != null)
						sendMessageToMultipleEdges(destinations.iterator(), new ControlledGHSMessage(vertex.getId(), connectedFragment, ControlledGHSMessage.ROOT_STATUS));
				}

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
			//			else if(vertexValue.isRoot()){
			for(ControlledGHSMessage msg : messages){
				PathfinderVertexID msgFragment = msg.getFragmentID();
				if(msg.getSenderID().equals(vertexValue.getFragmentIdentity())){
					if(isLogEnabled)
						log.info("updating last connected fragment -- removing from stack " + msgFragment);
					vertexValue.popSetOutOfStack(msgFragment);
				}
				//				}
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

			//			sendMessageToMultipleEdges(Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH, PathfinderEdgeType.DUMMY).iterator(), 
			//					new ControlledGHSMessage(vertex.getId(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ROOT_UPDATE));

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

	public static class BoruvkaRootUpdateCompletion extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(!vertexValue.boruvkaStatus() || vertex.getValue().isRoot())
				return;
			super.compute(vertex, messages);

			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			if(!msgs.hasNext())
				return;
			PathfinderVertexID newFragmentID = null;
			PathfinderVertexID oldFragment = vertexValue.getLastConnectedFragment();			
			PathfinderVertexID myFragment = vertexValue.getFragmentIdentity().copy();
			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID currentFragment = current.getFragmentID().copy();
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
				vertexValue.popSetOutOfStack(newFragmentID);
				//				vertexValue.setLastConnectedFragment(myFragment);
				oldFragment = myFragment;
				Iterable<PathfinderVertexID> dummyEdges = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.DUMMY);
				if(dummyEdges != null){
					Iterator<PathfinderVertexID> dummyEdgesIt = dummyEdges.iterator();
					while(dummyEdgesIt.hasNext()){
						PathfinderVertexID currentDummy = dummyEdgesIt.next();			//old pair of dummies are removed
//						log.info("Removing existing dummy " + currentDummy);
						Toolbox.removeExistingDummies(this, vertex, currentDummy.copy());
					}
				}
				//			if(msgs.hasNext()) what if is not the only message?
				//				throw new Exception();
				vertexValue.setFragmentIdentity(newFragmentID);
				//				if(vertex.getEdgeValue(newFragmentID) == null){
				//					Toolbox.connectWithDummies(this, vertex, newFragmentID);
				//				}
				if(vertex.getEdgeValue(newFragmentID) == null){
					Toolbox.connectWithDummies(this, vertex, newFragmentID);
//					log.info("Added new dummies to " + newFragmentID);
				}else if(vertex.getEdgeValue(newFragmentID).unassigned()){
					Toolbox.updateEdgeValueWithStatus(vertex, PathfinderEdgeType.DUMMY, newFragmentID);
					Toolbox.updateRemoteEdgeWithStatus(this, vertex.getId(), newFragmentID, vertex.getEdgeValue(newFragmentID), PathfinderEdgeType.DUMMY);
				}

			}else
				vertexValue.popSetOutOfStack(myFragment);


			if(oldFragment != null){
				vertexValue.popSetOutOfStack(oldFragment);
				if(/*vertexValue.isStackEmpty() && */!vertexValue.hasLOEsDepleted()){
					Collection<PathfinderVertexID> destinations = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.UNASSIGNED);
					if(destinations != null){
						if(isLogEnabled)
							log.info("Informing unassigned edges about my new fragment");
						sendMessageToMultipleEdges(destinations.iterator(), new ControlledGHSMessage(vertex.getId(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ROOT_UPDATE));
						sendMessageToMultipleEdges(destinations.iterator(), new ControlledGHSMessage(vertex.getId(), oldFragment, ControlledGHSMessage.ROOT_STATUS));					
					}
				}
				//				}else{
				//					log.info("Informing my loestack about my new fragment");
				//					for(Writable k : vertexValue.getActiveFragments()){
				//						sendMessageToMultipleEdges(((SetWritable<PathfinderVertexID>)vertexValue.getRecipientsForFragment((PathfinderVertexID)k)).iterator(), new ControlledGHSMessage(vertex.getId(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ROOT_UPDATE));
				//						sendMessageToMultipleEdges(((SetWritable<PathfinderVertexID>)vertexValue.getRecipientsForFragment((PathfinderVertexID)k)).iterator(), new ControlledGHSMessage(vertex.getId(), vertexValue.getLastConnectedFragment().copy(), ControlledGHSMessage.ROOT_STATUS));					
				//					}
				//				}
				//			}
				//			}
			}

		}

	}
}
