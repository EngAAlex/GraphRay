/**
 * 
 */
package com.graphray.ghs.computations;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
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


public class EdgeConnectionRoutine{

	protected static Logger log = Logger.getLogger(EdgeConnectionRoutine.class);

	MasterCompute master;
	int counter = 0;

	public EdgeConnectionRoutine(MasterCompute master){
		this.master = master;
	}

	public boolean compute() throws IOException{
		if(counter == 0 || counter == 1){
			master.setComputation(LOEConnectionSurvey.class);			
			counter++;
			return false;
		}else if(counter == 2){
			master.setComputation(ConnectionTestReply.class);
			counter++;
			return false;
		}else if(counter == 3){
			//			if(boruvkaConnection)
			//				master.setComputation(BoruvkaConnectionTestCompletion.class);
			//			else
			master.setComputation(ConnectionTestCompletion.class);
			counter++;
			//			master.setAggregatedValue(GraphRayMasterCompute.testMessages, new IntWritable(ControlledGHSMessage.CONNECT_MESSAGE));
			return false;
		}else if(counter == 4 || counter == 5){
			master.setComputation(LOEConnection.class);				
			counter++;
			return false;
		}else if(counter == 6){
			if(((BooleanWritable)master.getAggregatedValue(GraphRayMasterCompute.branchSkippedAggregator)).get())
				throw new IOException("No new branches were created!");
			master.setAggregatedValue(GraphRayMasterCompute.branchSkippedAggregator, new BooleanWritable(true));
			master.setComputation(BranchConnector.class);
			counter++;
			return false;
		}else if(counter == 7)
			counter = 0;		
		return true;
	}

	public static class LOEConnection extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			Iterator<ControlledGHSMessage> msgs = messages.iterator();					

			PathfinderVertexID vertexId = vertex.getId();
			//			PathfinderVertexID myLOEDestination = vertexValue.getLoeDestination();
			PathfinderVertexID myfragmentIdentity = vertexValue.getFragmentIdentity();			
			PathfinderVertexID selectedFragment = vertexValue.getLoeDestinationFragment();
			double myLOE = vertexValue.getLOE();

			HashSet<PathfinderVertexID> connections = new HashSet<PathfinderVertexID>();

			//			boolean branchConnection = false;

			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID msgSender = current.getSenderID();
				PathfinderVertexID msgFragmentIdentity = current.getFragmentID();				
				short msgStatus = current.getStatus();
				double msgLOE = current.get();

				if(msgStatus == ControlledGHSMessage.ROOT_UPDATE){
					rootUpdateMessageReceived(vertexValue, msgFragmentIdentity);
					continue;
				}
				if(msgStatus == ControlledGHSMessage.ROOT_STATUS){
					rootStatusMessageReceived(vertexId, vertexValue, msgFragmentIdentity);
					continue;
				}

				if(isLogEnabled)
					log.info("Received from " + msgSender + " identity " + msgFragmentIdentity);

				if(vertexValue.getFragmentIdentity().equals(msgSender) /*&& !vertex.getEdgeValue(myLOEDestination).isPathfinder()*/){ //CONNECT MESSAGE DID NOT CROSS THE FRAGMENT BORDER
					selectedFragment = msgFragmentIdentity.copy();
					vertexValue.setLoeDestinationFragment(selectedFragment);	
					if(isLogEnabled)
						log.info("Forwarding message to fragment recipients " + selectedFragment);						
					sendMessageToMultipleEdges(vertexValue.getRecipientsForFragment(selectedFragment).iterator(), 
							new ControlledGHSMessage(vertexId, myfragmentIdentity, vertexValue.getLOE(), ControlledGHSMessage.CONNECT_MESSAGE));
					aggregate(GraphRayMasterCompute.messagesLeftAggregator, new BooleanWritable(false));
					vertexValue.setPingedByRoot(true);
					if(msgStatus == ControlledGHSMessage.CONNECT_AS_BRANCH)
						vertexValue.authorizeBranchConnection();
				}else if(msgLOE == myLOE){ // if(vertexValue.loeStackContainsFragment(msgFragmentIdentity) /*myLOEDestination.equals(msgSender) || vertex.getEdgeValue(msgSender).isPathfinderCandidate()*/){ //CONNECT MESSAGE CROSSED THE FRAGMENT BORDER					
					PathfinderVertexID targetFragment = null;					
					switch(msgStatus){
					case ControlledGHSMessage.CONNECT_MESSAGE:
						targetFragment = msgFragmentIdentity;
						break;
					case ControlledGHSMessage.CONNECT_FROM_ROOT_MESSAGE: 
						targetFragment = msgSender;							
						break;
					case ControlledGHSMessage.CONNECT_AS_BRANCH:
						targetFragment = msgSender;						
						//						vertexValue.authorizeBranchConnection();
						break;
					}

					if(vertexValue.loeStackContainsFragment(targetFragment)){

						connections.add(targetFragment.copy());

						if(isLogEnabled)
							log.info("Accepting fragment " + targetFragment);

						if(msgStatus == ControlledGHSMessage.CONNECT_AS_BRANCH && targetFragment.equals(selectedFragment))
							vertexValue.authorizeBranchConnection();

						if(vertexValue.isRoot()){
							vertexValue.setPingedByRoot(true);
						}
					}
				}
			}

			if(vertexValue.isPingedByRoot() && !connections.isEmpty()){
				if(connections.contains(selectedFragment)/*!vertexValue.isStackEmpty()*/){
					if(isLogEnabled)
						log.info("Connecting to " + selectedFragment);
					//					if(!myfragmentIdentity.equals(selectedFragment))
					connect(vertex, vertexValue, selectedFragment);
					vertexValue.authorizeConnections();
				}

			}/*else*/

		}

		protected void connect(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexType vertexValue, PathfinderVertexID selectedFragment) throws IOException{
			if(!vertexValue.getFragmentIdentity().equals(selectedFragment)){
				SetWritable<PathfinderVertexID> edgesToUpdate = vertexValue.peekSetOutOfStack(selectedFragment);		
				Toolbox.setMultipleEdgesAsPathfinder(vertex, edgesToUpdate);
				rootUpdateNotification(vertex.getId(), vertex.getValue(), selectedFragment);
			}
			if(isLogEnabled)
				log.info("Connected with " + selectedFragment);
			aggregate(GraphRayMasterCompute.branchSkippedAggregator, new BooleanWritable(false));			
		}

		protected void rootUpdateMessageReceived(PathfinderVertexType vertexValue, PathfinderVertexID newRoot){
			if(!vertexValue.getFragmentIdentity().equals(newRoot)){
				if(isLogEnabled)
					log.info("Updating my Boruvka Root with " + newRoot);
				vertexValue.deactivateForBoruvka();
				vertexValue.setLastConnectedFragment(vertexValue.getFragmentIdentity());			
				vertexValue.setFragmentIdentity(newRoot.copy());
			}
		}

		protected void rootUpdateNotification(PathfinderVertexID vertexId, PathfinderVertexType vertexValue, PathfinderVertexID selectedFragment){
			if(vertexValue.getFragmentIdentity().get() < selectedFragment.get()){
				if(vertexValue.isRoot()){
					rootUpdateMessageReceived(vertexValue, selectedFragment);
				}else{
					if(isLogEnabled)
						log.info("informed my root " + vertexValue.getFragmentIdentity() + " to change the fragment identity");
					sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertexId, selectedFragment.copy(), ControlledGHSMessage.ROOT_UPDATE));
				}
			}else
				rootStatusMessageReceived(vertexId, vertexValue, selectedFragment);
		}

		public void rootStatusMessageReceived(PathfinderVertexID vertexId, PathfinderVertexType vertexValue, PathfinderVertexID selectedFragment){
			if(vertexValue.isRoot()){
				vertexValue.setLastConnectedFragment(selectedFragment.copy());
				vertexValue.popSetOutOfStack(selectedFragment);
				if(isLogEnabled)
					log.info("setting last connected fragment " + selectedFragment);
			}else{
				if(isLogEnabled)
					log.info("Informing my root of last connected fragment");
				sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertexId, selectedFragment.copy(), ControlledGHSMessage.ROOT_STATUS));
			}
		}

	}

	public static class LOEConnectionSurvey extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage> {


		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			Iterator<ControlledGHSMessage> msgs = messages.iterator();					

			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexID myfragmentIdentity = vertexValue.getFragmentIdentity();			
			Set<PathfinderVertexID> selectedFragmentIdentities = null;
			double myLOE = vertexValue.getLOE();

			//			HashSet<PathfinderVertexID> connections = new HashSet<PathfinderVertexID>();

			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID msgSender = current.getSenderID();
				PathfinderVertexID msgFragmentIdentity = current.getFragmentID();
				double msgValue = current.get();

				short msgStatus = current.getStatus();	

				if(vertexValue.getFragmentIdentity().equals(msgSender) /*&& !vertex.getEdgeValue(myLOEDestination).isPathfinder()*/){ //CONNECT MESSAGE DID NOT CROSS THE FRAGMENT BORDER
					if(isLogEnabled)
						log.info("Received from my root " + msgSender + "Forwarding message to fragment recipients " + msgFragmentIdentity);						
					sendMessageToMultipleEdges(vertexValue.getRecipientsForFragment(msgFragmentIdentity).iterator(), 
							new ControlledGHSMessage(vertexId, myfragmentIdentity, msgValue, msgStatus)); //MUST BE CONNECT TEST
					aggregate(GraphRayMasterCompute.messagesLeftAggregator, new BooleanWritable(false));
					vertexValue.setPingedByRoot(true);
					if(selectedFragmentIdentities == null)
						selectedFragmentIdentities = new HashSet<PathfinderVertexID>();
					selectedFragmentIdentities.add(msgFragmentIdentity.copy());
					//					vertexValue.acceptNewConnection(msgFragmentIdentity.copy());
				}else if(msgValue == myLOE && vertexValue.loeStackContainsFragment(msgFragmentIdentity)/*myLOEDestination.equals(msgSender) || vertex.getEdgeValue(msgSender).isPathfinderCandidate()*/){ //CONNECT MESSAGE CROSSED THE FRAGMENT BORDER
					if(isLogEnabled)
						log.info("Incoming connection from " + msgSender + " fragment " + msgFragmentIdentity);
					vertexValue.acceptNewConnection(msgFragmentIdentity.copy());

					if(vertexValue.isRoot()){
						vertexValue.setPingedByRoot(true);
					}
					//						proceed = true;
				}
			}
		}

	}

	public static class ConnectionTestReply extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

		protected short loesToDiscover;


		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.hasNoIncomingConnections() || !vertexValue.isPingedByRoot()){
				vertexValue.getReadyForNextRound();
				return;
			}
			SetWritable<PathfinderVertexID> acceptedConnections = vertexValue.getAcceptedConnections();
			if(/*!acceptedConnections.isEmpty() && */!vertexValue.isRoot()){
				if(isLogEnabled)
					log.info("Informing my root of cleared connection");
				for(PathfinderVertexID w : acceptedConnections/*vertexValue.getActiveFragments()*/)
					sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertex.getId(), w.copy(), vertexValue.getLOE(), ControlledGHSMessage.CONNECT_REPLY));
			}

		}
	}

	public static class ConnectionTestCompletion extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			if(!vertexValue.isRoot()){
				vertexValue.setPingedByRoot(false);
				vertexValue.deAuthorizeConnections();
				return;
			}
			Stack<PathfinderVertexID> verticesToInform = new Stack<PathfinderVertexID>();
			Collection<PathfinderVertexID> acceptedConnections = vertexValue.getAcceptedConnections();

			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			long maxFragment = Long.MIN_VALUE;
			double minLoe = Double.MAX_VALUE;
			PathfinderVertexID maxFragmentID = null;
			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID currentSenderID = current.getSenderID();
				PathfinderVertexID currentFragment = current.getFragmentID();
				double msgLoe = current.get();
				if(msgLoe <= minLoe){
					if(msgLoe < minLoe){
						maxFragment = Long.MIN_VALUE;
						verticesToInform.clear();
						minLoe = msgLoe;
					}
					if(currentFragment.get() >= maxFragment){
						if(currentFragment.get() > maxFragment){
							maxFragmentID = currentFragment.copy();
							maxFragment = currentFragment.get();
							//						operateOnVerticesToInform(verticesToInform);
							verticesToInform.clear();
						}
						//					extraOperationsOnSenderID(currentFragment, currentSenderID);
						verticesToInform.add(currentSenderID.copy());
					}
				}
			}

			SetWritable<PathfinderVertexID> myContribution = null;
			double myLOE = vertexValue.getLOE();
			acceptedConnections.retainAll(vertexValue.getActiveFragments());			
			if(acceptedConnections.size() > 0 && myLOE != Double.MAX_VALUE && myLOE <= minLoe){
				if(myLOE < minLoe){
					maxFragment = Long.MIN_VALUE;
					verticesToInform.clear();
					minLoe = myLOE;
				}
				//				Set<Writable> vertexConnections = vertexValue.getActiveFragments();
				//				for(Writable w : vertexConnections){
				//					PathfinderVertexID pfid = (PathfinderVertexID) w;
				for(PathfinderVertexID pfid : acceptedConnections){
					if(pfid.get() > maxFragment){
						maxFragment = pfid.get();
						maxFragmentID = pfid.copy();
						//					operateOnVerticesToInform(verticesToInform);					
						verticesToInform.clear();
					}
				}
				myContribution = vertexValue.peekSetOutOfStack(maxFragmentID);
			}

			if(maxFragmentID == null)
				return;

			if(isLogEnabled)
				log.info("Fragment chosen: " + maxFragmentID);
			vertexValue.setLoeDestinationFragment(maxFragmentID.copy());

			if(myContribution != null)
				verticesToInform.addAll(myContribution);
			if(isLogEnabled)
				log.info("Informing fragment " + maxFragmentID + " to enable connection. Vertices to inform " + verticesToInform.size());

			if(vertex.getId().get() >= maxFragmentID.get()){
				if(isLogEnabled)				
					log.info("I'm choosing who's gonna be the next branch");
				sendMessage(verticesToInform.pop(), new ControlledGHSMessage(vertex.getId(), maxFragmentID, minLoe, ControlledGHSMessage.CONNECT_AS_BRANCH));							
				//				informSoonToBeBranches(vertex.getId(), verticesToInform, minLoe, maxFragmentID);
			}

			if(verticesToInform.size() > 0)
				sendMessageToMultipleEdges(verticesToInform.iterator(), 
						new ControlledGHSMessage(vertex.getId(), maxFragmentID, minLoe, ControlledGHSMessage.CONNECT_FROM_ROOT_MESSAGE));

			//				if(!fragmentCount.containsKey(currenFragment.get()))
			//					fragmentCount.put(currenFragment.get(), 1);
			//				else
			//					fragmentCount.put(fragmentCount.get(currentFragment.get() + 1));
		}
		/**
		 * @param maxFragmentID
		 * @return
		 */
		protected SetWritable<PathfinderVertexID> getMyContribution(PathfinderVertexType vertexValue, PathfinderVertexID maxFragmentID) {
			return vertexValue.peekSetOutOfStack(maxFragmentID);
		}
		/**
		 * @param currentFragment
		 * @param currentSenderID
		 */
		protected void extraOperationsOnSenderID(PathfinderVertexID currentFragment, PathfinderVertexID currentSenderID) {
			//NO-OP
		}
		//			if(vertexValue.isClearedForConnection() && !vertexValue.isRoot())
		//				sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertex.getId(), vertexValue.getLoeDestinationFragment(), ControlledGHSMessage.CONNECT_REPLY));

		/**
		 * @param verticesToInform
		 * @param maxFragmentID
		 */
		protected void informSoonToBeBranches(PathfinderVertexID vertexID, Stack<PathfinderVertexID> verticesToInform, PathfinderVertexID maxFragmentID) {
			sendMessage(verticesToInform.pop(), new ControlledGHSMessage(vertexID, maxFragmentID, ControlledGHSMessage.CONNECT_AS_BRANCH));			
		}


		public void operateOnVerticesToInform(Stack<PathfinderVertexID> vti){
			vti.clear();
		}
	}

	public static class BranchConnector extends LOEConnection {

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.isRoot()){
				PathfinderVertexID receivedNewPfid = null;
				Iterator<ControlledGHSMessage> msgs = messages.iterator();
				while(msgs.hasNext()){
					ControlledGHSMessage current = msgs.next();
					PathfinderVertexID msgsFragmentID = current.getFragmentID();
					if(current.getStatus() == ControlledGHSMessage.ROOT_STATUS){
						rootStatusMessageReceived(vertex.getId(), vertexValue, msgsFragmentID);
						continue;
					}					
					if(receivedNewPfid == null)
						receivedNewPfid = msgsFragmentID;
					else if(!receivedNewPfid.equals(msgsFragmentID))
						throw new IOException("Two different IDS for Boruvka root update!");
				}
				if(receivedNewPfid != null)
					rootUpdateMessageReceived(vertexValue, receivedNewPfid);
			}
			if(!vertexValue.isClearedForConnection() || !vertexValue.isBranchConnectionEnabled()){
//				vertexValue.popSetOutOfStack(vertexValue.getLoeDestinationFragment());				
				return;
			}
			//			if(vertexValue.stackSize() != 1)
			//				throw new IOException("Stack size for branch connection != 1");
			//			PathfinderVertexID selectedNeighbor = vertexValue.popSetOutOfStack(
			//					(PathfinderVertexID) vertexValue.getActiveFragments().iterator().next()).pop().copy();
			if(isLogEnabled)
				log.info("I'm gonna connect to fragment " + vertexValue.getLoeDestinationFragment() + " as branch");
			PathfinderVertexID selectedNeighbor = vertexValue.peekSetOutOfStack(
					vertexValue.getLoeDestinationFragment()).pop().copy();			
			Toolbox.setEdgeAsBranch(vertex, selectedNeighbor);	
			Toolbox.setRemoteEdgeAsBranch(this, vertex.getId(), vertex.getEdgeValue(selectedNeighbor), selectedNeighbor);
		}
	}

}
