/**
 * 
 */
package unipg.pathfinder.ghs.computations;

import java.io.IOException;
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

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
import unipg.pathfinder.common.writables.SetWritable;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.utils.Toolbox;


public class EdgeConnectionRoutine{

	protected static Logger log = Logger.getLogger(EdgeConnectionRoutine.class);

	MasterCompute master;
	int counter = 0;

	public EdgeConnectionRoutine(MasterCompute master){
		this.master = master;
	}

	public boolean compute(){
		//		if(counter == 0)
		//			master.setAggregatedValue(MSTPathfinderMasterCompute.testMessages, new IntWritable(ControlledGHSMessage.CONNECT_TEST));

		if(counter == 0 || counter == 1){
			master.setComputation(LOEConnectionSurvey.class);
			counter++;
			return false;
		}else if(counter == 2){
			master.setComputation(ConnectionTestReply.class);
			counter++;
			return false;
		}else if(counter == 3){
			master.setComputation(ConnectionTestCompletion.class);
			counter++;
			//			master.setAggregatedValue(MSTPathfinderMasterCompute.testMessages, new IntWritable(ControlledGHSMessage.CONNECT_MESSAGE));
			return false;
		}else if(counter == 4 || counter == 5){
			master.setComputation(LOEConnection.class);
			counter++;
			return false;
		}else if(counter == 6){
			master.setComputation(BranchConnector.class);
			counter++;
			return false;
		}else if(counter == 7)
			counter = 0;		
		return true;
	}

	public static class LOEConnection extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

		protected short loesToDiscover;
		protected boolean boruvkaConnection;


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

			HashSet<PathfinderVertexID> connections = new HashSet<PathfinderVertexID>();

			//			boolean branchConnection = false;

			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID msgSender = current.getSenderID();
				PathfinderVertexID msgFragmentIdentity = current.getFragmentID();

				short msgStatus = current.getStatus();
				if(msgStatus == ControlledGHSMessage.ROOT_UPDATE){
					rootUpdateMessageReceived(vertexValue, msgFragmentIdentity);
					return;
				}			

				//				if(vertexValue.isStackEmpty()){
				//					log.info("No neighbors in stack");
				//					vertexValue.resetLOE();
				//					break;
				//				}

				//				log.info("Received from " + msgSender + " identity " + msgFragmentIdentity);

				if(vertexValue.getFragmentIdentity().equals(msgSender) /*&& !vertex.getEdgeValue(myLOEDestination).isPathfinder()*/){ //CONNECT MESSAGE DID NOT CROSS THE FRAGMENT BORDER
					selectedFragment = msgFragmentIdentity.copy();
					vertexValue.setLoeDestinationFragment(selectedFragment);					
					log.info("Forwarding message to fragment recipients " + selectedFragment);						
					sendMessageToMultipleEdges(vertexValue.getRecipientsForFragment(selectedFragment).iterator(), 
							new ControlledGHSMessage(vertexId, myfragmentIdentity, ControlledGHSMessage.CONNECT_MESSAGE));
					aggregate(MSTPathfinderMasterCompute.messagesLeftAggregator, new BooleanWritable(false));
					vertexValue.setPingedByRoot(true);
					if(msgStatus == ControlledGHSMessage.CONNECT_AS_BRANCH)
						vertexValue.authorizeBranchConnection();
				}else if(/*myLOEDestination.equals(msgSender) ||*/ vertex.getEdgeValue(msgSender).isPathfinderCandidate()){ //CONNECT MESSAGE CROSSED THE FRAGMENT BORDER
					log.info("Incoming connection from " + msgSender);
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
						vertexValue.authorizeBranchConnection();
						break;
					}

					connections.add(targetFragment.copy());

					log.info("Target fragment " + targetFragment);

					if(vertexValue.isRoot()){
						vertexValue.setPingedByRoot(true);
						//						if(vertexValue.getLoeDestinationFragment().equals(targetFragment))
						//							selectedFragment = targetFragment.copy();						
						//						selectedFragment = vertexValue.getLoeDestinationFragment();
					}
				}
			}

			if(vertexValue.isPingedByRoot() && !connections.isEmpty()){
				//				if(selectedFragment == null)
				//				vertexValue.authorizeConnections();
				//				for(PathfinderVertexID pf : connections)
				//					if(!pf.equals(selectedFragment)){
				//						SetWritable<PathfinderVertexID> toRemove = vertexValue.popSetOutOfStack(pf);
				//						Toolbox.updateMultipleEdgeValueWithStatus(vertex, loesToDiscover, toRemove);
				//					}

				if(connections.contains(selectedFragment)/*!vertexValue.isStackEmpty()*/){
					log.info("Connecting to " + selectedFragment);
					connect(vertex, selectedFragment);
					vertexValue.authorizeConnections();
				}

			}/*else*/

		}

		protected void connect(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID selectedFragment) throws IOException{
			Toolbox.armFragmentPathfinderCandidates(vertex, selectedFragment);
			if(boruvkaConnection)
				rootUpdateNotification(vertex.getId(), vertex.getValue(), selectedFragment);				
			log.info("Connected with " + selectedFragment);
		}
		
		protected void rootUpdateMessageReceived(PathfinderVertexType vertexValue, PathfinderVertexID newRoot){
			log.info("Updating my Boruvka Root with " + newRoot + " using a Root update message");
			vertexValue.deactivateForBoruvka();
			vertexValue.setFragmentIdentity(newRoot.copy());
		}

		protected void rootUpdateNotification(PathfinderVertexID vertexId, PathfinderVertexType vertexValue, PathfinderVertexID selectedFragment){
			if(vertexValue.getFragmentIdentity().get() < selectedFragment.get()){
				if(vertexValue.isRoot()){
					vertexValue.deactivateForBoruvka();
					vertexValue.setFragmentIdentity(selectedFragment.copy());
					log.info("Updating my Boruvka Root with " + vertexValue.getFragmentIdentity().get());						
				}else{
					log.info("informed my root " + vertexValue.getFragmentIdentity() + " to change the fragment identity");
					sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertexId, selectedFragment.copy(), ControlledGHSMessage.ROOT_UPDATE));
				}
			}
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.bsp.CentralizedServiceWorker, org.apache.giraph.worker.WorkerGlobalCommUsage)
		 */
		@Override
		public void initialize(GraphState graphState,
				WorkerClientRequestProcessor<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> workerClientRequestProcessor,
				CentralizedServiceWorker<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> serviceWorker,
				WorkerGlobalCommUsage workerGlobalCommUsage) {
			super.initialize(graphState, workerClientRequestProcessor, serviceWorker, workerGlobalCommUsage);
			boruvkaConnection = ((IntWritable)getAggregatedValue(MSTPathfinderMasterCompute.loesToDiscoverAggregator)).get() == PathfinderEdgeType.INTERFRAGMENT_EDGE;
			loesToDiscover = (short) ((IntWritable)getAggregatedValue(MSTPathfinderMasterCompute.loesToDiscoverAggregator)).get();
		}

	}

	public static class LOEConnectionSurvey extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage> {

		protected short loesToDiscover;
//		protected boolean boruvkaConnection;
		//		protected boolean encore = false;


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

			//			HashSet<PathfinderVertexID> connections = new HashSet<PathfinderVertexID>();

			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID msgSender = current.getSenderID();
				PathfinderVertexID msgFragmentIdentity = current.getFragmentID();

				short msgStatus = current.getStatus();	

				//				if(vertexValue.isStackEmpty() /*msgStatus == ControlledGHSMessage.REFUSE_MESSAGE || myLOEDestination == null || vertex.getEdgeValue(myLOEDestination).isPathfinder()*/){
				//					log.info("Received a refuse message or edge already pathfinder or myLOEDestination null");
				//					vertexValue.resetLOE();
				//					break;
				//				}

				log.info("Received from " + msgSender + " identity " + msgFragmentIdentity);

				if(vertexValue.getFragmentIdentity().equals(msgSender) /*&& !vertex.getEdgeValue(myLOEDestination).isPathfinder()*/){ //CONNECT MESSAGE DID NOT CROSS THE FRAGMENT BORDER
					log.info("Received from my root " + msgSender + "Forwarding message to fragment recipients " + msgFragmentIdentity);						
					sendMessageToMultipleEdges(vertexValue.getRecipientsForFragment(msgFragmentIdentity).iterator(), 
							new ControlledGHSMessage(vertexId, myfragmentIdentity, msgStatus)); //MUST BE CONNECT TEST
					aggregate(MSTPathfinderMasterCompute.messagesLeftAggregator, new BooleanWritable(false));
					vertexValue.setPingedByRoot(true);
					if(selectedFragmentIdentities == null)
						selectedFragmentIdentities = new HashSet<PathfinderVertexID>();
					selectedFragmentIdentities.add(msgFragmentIdentity.copy());
					log.info("Adding to selected fragment identitites " + msgFragmentIdentity);
					//					vertexValue.acceptNewConnection(msgFragmentIdentity.copy());
				}else if(/*myLOEDestination.equals(msgSender) ||*/ vertex.getEdgeValue(msgSender).isPathfinderCandidate()){ //CONNECT MESSAGE CROSSED THE FRAGMENT BORDER
					log.info("Incoming connection from " + msgSender + " fragment " + msgFragmentIdentity);
					vertexValue.acceptNewConnection(msgFragmentIdentity.copy());

					if(vertexValue.isRoot()){
						vertexValue.setPingedByRoot(true);
					}
					//						proceed = true;
				}
			}

			if(vertexValue.isPingedByRoot() && selectedFragmentIdentities != null){
				//				if(!connections.isEmpty()){
				HashSet<PathfinderVertexID> toRemove = null;			
				for(Writable w : vertexValue.getActiveFragments()){
					PathfinderVertexID candidate = (PathfinderVertexID) w;
					log.info("checking for candidate " + candidate);						
					if(!selectedFragmentIdentities.contains(candidate)){
						if(toRemove == null)
							toRemove = new HashSet<PathfinderVertexID>();
						log.info("Adding to remove set");
						toRemove.add(candidate);
					}
				}
				if(toRemove != null)
					for(PathfinderVertexID id : toRemove)
						Toolbox.removeSetFromActiveFragmentStack(vertex, id, loesToDiscover);

				//				}
				//				if(!vertexValue.isStackEmpty())
				//					vertexValue.authorizeConnections();				
				//vertexValue.resetLOEStack();
			}
		}


		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.bsp.CentralizedServiceWorker, org.apache.giraph.worker.WorkerGlobalCommUsage)
		 */
		@Override
		public void initialize(GraphState graphState,
				WorkerClientRequestProcessor<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> workerClientRequestProcessor,
				CentralizedServiceWorker<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> serviceWorker,
				WorkerGlobalCommUsage workerGlobalCommUsage) {
			super.initialize(graphState, workerClientRequestProcessor, serviceWorker, workerGlobalCommUsage);
//			boruvkaConnection = ((IntWritable)getAggregatedValue(MSTPathfinderMasterCompute.loesToDiscoverAggregator)).get() == PathfinderEdgeType.INTERFRAGMENT_EDGE;
			//			messagesToListen = (short) ((IntWritable)getAggregatedValue(MSTPathfinderMasterCompute.testMessages)).get();
			loesToDiscover = (short) ((IntWritable)getAggregatedValue(MSTPathfinderMasterCompute.loesToDiscoverAggregator)).get();
		}
	}

	//	public static class LOEConnectionEncore extends LOEConnectionSurvey { //this is necessary to ensure that each vertex
	//		//is aware of its branches
	//
	//		/* (non-Javadoc)
	//		 * @see unipg.pathfinder.ghs.computations.GHSComputations.LOEConnection#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.bsp.CentralizedServiceWorker, org.apache.giraph.worker.WorkerGlobalCommUsage)
	//		 */
	//		@Override
	//		public void initialize(GraphState graphState,
	//				WorkerClientRequestProcessor<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> workerClientRequestProcessor,
	//				CentralizedServiceWorker<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> serviceWorker,
	//				WorkerGlobalCommUsage workerGlobalCommUsage) {
	//			super.initialize(graphState, workerClientRequestProcessor, serviceWorker, workerGlobalCommUsage);
	//			encore = true;
	//		}
	//	}

	public static class ConnectionTestReply extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

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
				vertexValue.resetLOEStack();
				return;
			}
			SetWritable<PathfinderVertexID> acceptedConnections = vertexValue.getAcceptedConnections();
			HashSet<PathfinderVertexID> toRemove = new HashSet<PathfinderVertexID>();				
			for(Writable w : vertexValue.getActiveFragments()){
				PathfinderVertexID candidate = (PathfinderVertexID) w;
				log.info("Checking " + candidate + " for eligibility");
				if(!acceptedConnections.contains(candidate)){
					toRemove.add(candidate.copy());
					log.info("Removing");
				}
			}
			for(PathfinderVertexID p : toRemove)
				Toolbox.removeSetFromActiveFragmentStack(vertex, p, loesToDiscover);
			vertexValue.clearAcceptedConnections();
			if(!vertexValue.isStackEmpty() && !vertexValue.isRoot()){
				log.info("Informing my root of cleared connection");
				for(Writable w : vertexValue.getActiveFragments())
					sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertex.getId(), ((PathfinderVertexID)w).copy(), ControlledGHSMessage.CONNECT_REPLY));
			}
		}	

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
		}
	}

	public static class ConnectionTestCompletion extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

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
			//			HashMap<PathfinderVertexID, HashSet<PathfinderVertexID>> fragmentCount = new HashMap<PathfinderVertexID, HashSet<PathfinderVertexID>>();
			Stack<PathfinderVertexID> verticesToInform = new Stack<PathfinderVertexID>();

			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			long maxFragment = Long.MIN_VALUE;
			PathfinderVertexID maxFragmentID = null;
			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID currentSenderID = current.getSenderID();
				PathfinderVertexID currentFragment = current.getFragmentID();
				if(currentFragment.get() >= maxFragment){
					if(currentFragment.get() > maxFragment){
						maxFragmentID = currentFragment.copy();
						maxFragment = currentFragment.get();
						verticesToInform.clear();
					}
					verticesToInform.add(currentSenderID.copy());
				}
			}
			Set<Writable> vertexConnections = vertexValue.getActiveFragments();
			for(Writable w : vertexConnections){
				PathfinderVertexID pfid = (PathfinderVertexID) w;
				if(pfid.get() > maxFragment){
					maxFragment = pfid.get();
					maxFragmentID = pfid.copy();
					verticesToInform.clear();
				}
			}

			if(maxFragmentID == null)
				return;

			log.info("Fragment chosen: " + maxFragmentID);
			vertexValue.setLoeDestinationFragment(maxFragmentID.copy());

			SetWritable<PathfinderVertexID> myContribution = vertexValue.getSetOutOfStack(maxFragmentID);

			if(myContribution != null)
				verticesToInform.addAll(myContribution);
			log.info("Informing fragment " + maxFragmentID + " to enable connection. Vertex to inform " + verticesToInform.size());

			if(vertex.getId().get() >= maxFragmentID.get()){
				log.info("I'm choosing who's gonna be the next branch");
				sendMessage(verticesToInform.pop(), new ControlledGHSMessage(vertex.getId(), maxFragmentID, ControlledGHSMessage.CONNECT_AS_BRANCH));
			}

			if(verticesToInform.size() > 0)
				sendMessageToMultipleEdges(verticesToInform.iterator(), 
						new ControlledGHSMessage(vertex.getId(), maxFragmentID, ControlledGHSMessage.CONNECT_FROM_ROOT_MESSAGE));

			//				if(!fragmentCount.containsKey(currenFragment.get()))
			//					fragmentCount.put(currenFragment.get(), 1);
			//				else
			//					fragmentCount.put(fragmentCount.get(currentFragment.get() + 1));
		}
		//			if(vertexValue.isClearedForConnection() && !vertexValue.isRoot())
		//				sendMessage(vertexValue.getFragmentIdentity(), new ControlledGHSMessage(vertex.getId(), vertexValue.getLoeDestinationFragment(), ControlledGHSMessage.CONNECT_REPLY));
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
					PathfinderVertexID msgsFragmentID = msgs.next().getFragmentID();
					if(receivedNewPfid == null)
						receivedNewPfid = msgsFragmentID;
					else if(!receivedNewPfid.equals(msgsFragmentID))
						throw new IOException("Two different IDS for Boruvka root update!");
				}
				if(receivedNewPfid != null)
					rootUpdateMessageReceived(vertexValue, receivedNewPfid);
			}
			if(!vertexValue.isClearedForConnection() || !vertexValue.isBranchConnectionEnabled())
				return;
			//			if(vertexValue.stackSize() != 1)
			//				throw new IOException("Stack size for branch connection != 1");
			//			PathfinderVertexID selectedNeighbor = vertexValue.popSetOutOfStack(
			//					(PathfinderVertexID) vertexValue.getActiveFragments().iterator().next()).pop().copy();
			PathfinderVertexID selectedNeighbor = vertexValue.popSetOutOfStack(
					vertexValue.getLoeDestinationFragment()).pop().copy();			
			Toolbox.setEdgeAsBranch(vertex, selectedNeighbor);	
			log.info("connecting to " + selectedNeighbor);
			Toolbox.setRemoteEdgeAsBranch(this, vertex.getId(), vertex.getEdgeValue(selectedNeighbor), selectedNeighbor);
		}

	}

}
