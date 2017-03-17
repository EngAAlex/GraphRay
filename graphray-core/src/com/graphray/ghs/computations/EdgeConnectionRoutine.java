/**
 * 
 */
package com.graphray.ghs.computations;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;
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
			////			SetWritable<PathfinderVertexID> selectedFragments;
			//			if(vertexValue.isRoot())
			//				selectedFragments = vertexValue.getLoeDestinationFragments();
			double myLOE = vertexValue.getLOE();

			HashSet<PathfinderVertexID> connections = new HashSet<PathfinderVertexID>();
			Set<PathfinderVertexID> branchEnabled = null;
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
					PathfinderVertexID currentSelectedFragment = msgFragmentIdentity.copy();
					vertexValue.addToLoeDestinationFragment(currentSelectedFragment);	
					if(isLogEnabled)
						log.info("Forwarding message to fragment recipients " + currentSelectedFragment);						
					sendMessageToMultipleEdges(vertexValue.getRecipientsForFragment(currentSelectedFragment).iterator(), 
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

						if(msgStatus == ControlledGHSMessage.CONNECT_AS_BRANCH/* && selectedFragments.contains(targetFragment)*/){
//							if(branchEnabled == null)
//								branchEnabled = new HashSet<PathfinderVertexID>();
//							branchEnabled.add(targetFragment);
							vertexValue.acceptNewConnection(targetFragment);
						}
						//							vertexValue.authorizeBranchConnection();

						if(vertexValue.isRoot()){
							vertexValue.setPingedByRoot(true);
						}
					}
				}
			}

			if(vertexValue.isPingedByRoot() && !connections.isEmpty()){
				connections.retainAll(vertexValue.getLoeDestinationFragments());
				if(!connections.isEmpty()){
					for(PathfinderVertexID pfid : connections){
						if(isLogEnabled)
							log.info("Connecting to " + pfid);
						//					if(!myfragmentIdentity.equals(selectedFragment))
						connect(vertex, vertexValue, pfid);
					}
					vertexValue.authorizeConnections();
					vertexValue.retainAcceptedConnections(connections);
//					if(branchEnabled != null){
//						branchEnabled.retainAll(connections);
//						if(!branchEnabled.isEmpty())
//							vertexValue.authorizeBranchConnection();
//					}
				}
			}

		}/*else*/


		protected void connect(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexType vertexValue, PathfinderVertexID selectedFragment) throws IOException{
//			if(!vertexValue.getFragmentIdentity().equals(selectedFragment)){
				SetWritable<PathfinderVertexID> edgesToUpdate = vertexValue.peekSetOutOfStack(selectedFragment);		
				Toolbox.setMultipleEdgesAsPathfinder(vertex, edgesToUpdate);
				rootUpdateNotification(vertex.getId(), vertex.getValue(), selectedFragment);
//			}
			if(isLogEnabled)
				log.info("Connected with " + selectedFragment);
			aggregate(GraphRayMasterCompute.branchSkippedAggregator, new BooleanWritable(false));			
		}

		protected void rootUpdateMessageReceived(PathfinderVertexType vertexValue, PathfinderVertexID newRoot){
			if(!vertexValue.getFragmentIdentity().equals(newRoot)){
				if(isLogEnabled)
					log.info("Updating my Boruvka Root with " + newRoot);
				vertexValue.deactivateForBoruvka();
				vertexValue.setOldFragmentID(vertexValue.getFragmentIdentity());			
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
				vertexValue.addToLastConnectedFragments(selectedFragment.copy());
//				vertexValue.popSetOutOfStack(selectedFragment);
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
//			Set<PathfinderVertexID> selectedFragmentIdentities = null;
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
//					if(selectedFragmentIdentities == null)
//						selectedFragmentIdentities = new HashSet<PathfinderVertexID>();
//					selectedFragmentIdentities.add(msgFragmentIdentity.copy());
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
				PathfinderVertexID myFragmentIdentity = vertexValue.getFragmentIdentity();
				for(PathfinderVertexID w : acceptedConnections/*vertexValue.getActiveFragments()*/){
					if(isLogEnabled)
						log.info("Informing my root " + myFragmentIdentity + " of cleared connection to " + w);
					if(!w.equals(myFragmentIdentity))
						sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertex.getId(), w.copy(), vertexValue.getLOE(), ControlledGHSMessage.CONNECT_REPLY));
				}				
			}

		}
	}

	public static class ConnectionTestCompletion extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@SuppressWarnings("unchecked")
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
			SetWritable<PathfinderVertexID> verticesToInform = new SetWritable<PathfinderVertexID>();
			MapWritable alternativeVerticesToInform = new MapWritable();

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
						alternativeVerticesToInform.clear();
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
					if(!alternativeVerticesToInform.containsKey(currentFragment))
						alternativeVerticesToInform.put(currentFragment.copy(), new SetWritable<PathfinderVertexID>());
					((SetWritable<PathfinderVertexID>)alternativeVerticesToInform.get(currentFragment)).add(currentSenderID.copy());
				}
			}

			//			SetWritable<PathfinderVertexID> myContribution = null;

			double myLOE = vertexValue.getLOE();
			acceptedConnections.retainAll(vertexValue.getActiveFragments());			
			if(acceptedConnections.size() > 0 && myLOE != Double.MAX_VALUE && myLOE <= minLoe){
				if(myLOE < minLoe){
					maxFragment = Long.MIN_VALUE;
					verticesToInform.clear();
					alternativeVerticesToInform.clear();
					minLoe = myLOE;
				}

				for(PathfinderVertexID pfid : acceptedConnections){
					if(pfid.get() > maxFragment){
						maxFragment = pfid.get();
						maxFragmentID = pfid.copy();
						verticesToInform.clear();
					}
					if(!alternativeVerticesToInform.containsKey(pfid))
						alternativeVerticesToInform.put(pfid.copy(), new SetWritable<PathfinderVertexID>());
					((SetWritable<PathfinderVertexID>)alternativeVerticesToInform.get(pfid))
						.addIfNotExistingAll(vertexValue.getRecipientsForFragment(pfid));
				}
			}

			if(maxFragmentID == null)
				return;
			
			vertexValue.setFragmentLoeValue(minLoe);
			
			if(maxFragmentID.get() > vertexValue.getFragmentIdentity().get()){
				attemptToConnectFragment(vertex, verticesToInform, minLoe, maxFragmentID);
				alternativeVerticesToInform.clear();
			}else{
				for(Writable w : alternativeVerticesToInform.keySet()){
					PathfinderVertexID currentFragmentToMerge = (PathfinderVertexID) w;
					attemptToConnectFragment(vertex, ((SetWritable<PathfinderVertexID>)alternativeVerticesToInform.get(w)), minLoe, currentFragmentToMerge);
				}
			}

			vertexValue.deAuthorizeConnections();
		}


		protected void attemptToConnectFragment(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, SetWritable<PathfinderVertexID> verticesToInform, double minLoe, PathfinderVertexID fragment){

			PathfinderVertexType vertexValue = vertex.getValue();

			if(isLogEnabled)
				log.info("Fragment chosen: " + fragment);
			vertexValue.addToLoeDestinationFragment(fragment.copy());

			SetWritable<PathfinderVertexID>myContribution = vertexValue.peekSetOutOfStack(fragment);

			if(myContribution != null)
				verticesToInform.addAll(myContribution);
			if(isLogEnabled)
				log.info("Informing fragment " + fragment + " to enable connection. Vertices to inform " + verticesToInform.size());

			if(vertex.getId().get() >= fragment.get()){
				if(isLogEnabled)				
					log.info("I'm choosing who's gonna be the next branch");
				sendMessage(verticesToInform.pop(), new ControlledGHSMessage(vertex.getId(), fragment, minLoe, ControlledGHSMessage.CONNECT_AS_BRANCH));							
				//				informSoonToBeBranches(vertex.getId(), verticesToInform, minLoe, maxFragmentID);
			}

			if(verticesToInform.size() > 0)
				sendMessageToMultipleEdges(verticesToInform.iterator(), 
						new ControlledGHSMessage(vertex.getId(), fragment, minLoe, ControlledGHSMessage.CONNECT_FROM_ROOT_MESSAGE));

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
			
			SetWritable<PathfinderVertexID> branchCandidates = vertexValue.getAcceptedConnections();
			for(PathfinderVertexID candidate : branchCandidates){
				if(isLogEnabled)
					log.info("I'm gonna connect to fragment " + candidate + " as branch");
				PathfinderVertexID selectedNeighbor = vertexValue.peekSetOutOfStack(candidate).peek();
				Toolbox.setEdgeAsBranch(vertex, selectedNeighbor);	
				Toolbox.setRemoteEdgeAsBranch(this, vertex.getId(), vertex.getEdgeValue(selectedNeighbor), selectedNeighbor);
			}
		}
	}

}
