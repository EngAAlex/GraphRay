/**
 * 
 */
package unipg.pathfinder.boruvka.computations;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;
import unipg.pathfinder.utils.Toolbox;

/**
 * @author spark
 *
 */
public class BoruvkaComputations {

	public static class BoruvkaSetup extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			vertex.getValue().resetLOE();
			vertex.getValue().resetLOEDepleted();
			if(vertex.getValue().isRoot())
				log.info("New root");
			else
				log.info("Belong to " + vertex.getValue().getFragmentIdentity());
			//			if(!vertex.getValue().isRoot()){
			//				PathfinderVertexID fragmentIdentity = vertex.getValue().getFragmentIdentity();
			//				Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
			//				while(edges.hasNext()){
			//					Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
			//					if(!current.getTargetVertexId().equals(fragmentIdentity))
			//						current.getValue().revertToUnassigned();
			//				}
			//			}
		}
	}

	public static class BoruvkaPathfinderUpdatePing extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			// TODO Auto-generated method stub
			super.compute(vertex, messages);
			Collection<PathfinderVertexID> destinationsIterable = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.PATHFINDER);
			if(destinationsIterable != null){
				sendMessageToMultipleEdges(destinationsIterable.iterator(), new ControlledGHSMessage(vertex.getId(), vertex.getValue().getFragmentIdentity(), ControlledGHSMessage.REPORT_MESSAGE));
			}
		}

	}

	public static class BoruvkaPathfinderUpdateReply extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				if(!current.getFragmentID().equals(vertexValue.getFragmentIdentity())){
					log.info("updating pathfinder as interfragment edge");
					PathfinderVertexID remoteID = current.getSenderID().copy();
					Toolbox.updateEdgeValueWithStatus(vertex, PathfinderEdgeType.INTERFRAGMENT_EDGE, remoteID);
					Toolbox.updateRemoteEdgeWithStatus(this, vertex.getId(), remoteID, vertex.getEdgeValue(remoteID), PathfinderEdgeType.INTERFRAGMENT_EDGE);
				}
			}
		}
	}

	public static class BoruvkaRootUpdateSetup extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

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
				if(!vertexValue.hasLOEsDepleted()){
					log.info("Adding edge as dummy to " + myFragment);					
					if(vertex.getEdgeValue(myFragment) == null){
						Toolbox.connectWithDummies(this, vertex, myFragment);
					}
					log.info("LOES not depleted");
				}else
					vertexValue.setRoot(false); //the vertex will remain silent from now on				
			}else if(vertexValue.isRoot()){
				log.info("I'm still a root during Boruvka");
				aggregate(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator, new IntWritable(1));
			}
		}

	}

	public static class BoruvkaRootUpdateConfirmationPing extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

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
				log.info("Asking for confirmation to " + myFragment);
				sendMessage(myFragment, new ControlledGHSMessage(vertex.getId(), myFragment, ControlledGHSMessage.ROOT_STATUS));
				vertexValue.setRoot(false);
				vertexValue.reactivateForBoruvka();
			}
		}
	}

	public static class BoruvkaRootUpdateConfirmationReply extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

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
				if(!current.getFragmentID().equals(myFragment)){
					if(verticesToNotify == null)
						verticesToNotify = new HashSet<PathfinderVertexID>();
					log.info("Notifying " + current.getSenderID() + " of updated root " + myFragment);
					verticesToNotify.add(current.getSenderID().copy());
				}else
					log.info("Fragment match -- no need for update");
			}

			if(verticesToNotify != null)
				sendMessageToMultipleEdges(verticesToNotify.iterator(), new ControlledGHSMessage(vertex.getId(), myFragment, ControlledGHSMessage.ROOT_UPDATE));

			//			sendMessageToMultipleEdges(Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH, PathfinderEdgeType.DUMMY).iterator(), 
			//					new ControlledGHSMessage(vertex.getId(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ROOT_UPDATE));

		}
	}

	public static class BoruvkaRootUpdateConfirmationCompletion extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

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
						log.info("Updating with correct fragment " + current.getFragmentID());
						Toolbox.removeExistingDummies(this, vertex, myFragment);
						vertexValue.setFragmentIdentity(current.getFragmentID().copy());
					}
				}
			else				
				log.info("Fragment already " + myFragment);

			Collection<PathfinderVertexID> verticesToNotify = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH, PathfinderEdgeType.DUMMY);

			if(verticesToNotify != null)
				sendMessageToMultipleEdges(verticesToNotify.iterator(), new ControlledGHSMessage(vertex.getId(), myFragment, ControlledGHSMessage.ROOT_UPDATE));

		}


	}

	public static class BoruvkaRootUpdateCompletion extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

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
			PathfinderVertexID myFragment = vertexValue.getFragmentIdentity();
			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID currentFragment = current.getFragmentID().copy();
				log.info("Received " + currentFragment + " from " + current.getSenderID());
				if(myFragment.equals(currentFragment)){
					log.info("Fragment already  " + currentFragment);
					continue;
					//					return;
				}else if(currentFragment.equals(vertex.getId())){
					log.info("discarding self id -- received from " + current.getSenderID());
					//				removeEdgesRequest(vertex.getId(), vertex.getId());
					//					return;
					continue;
				}
				if(myFragment.get() < currentFragment.get() && (newFragmentID == null || newFragmentID.get() < currentFragment.get())){
					log.info("Updating with new  fragment");
					newFragmentID = currentFragment;
				}
			}
			
			if(newFragmentID == null)
				return;
			
			log.info("Updating with new fragment " + newFragmentID);
			Iterable<PathfinderVertexID> dummyEdges = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.DUMMY);
			if(dummyEdges != null){
				Iterator<PathfinderVertexID> dummyEdgesIt = dummyEdges.iterator();
				while(dummyEdgesIt.hasNext()){
					PathfinderVertexID currentDummy = dummyEdgesIt.next();			//old pair of dummies are removed
					log.info("Removing existing dummy " + currentDummy);
					Toolbox.removeExistingDummies(this, vertex, currentDummy.copy());
				}
			}
			//			if(msgs.hasNext()) what if is not the only message?
			//				throw new Exception();
			vertexValue.setFragmentIdentity(newFragmentID);
			if(vertex.getEdgeValue(newFragmentID) == null){
				Toolbox.connectWithDummies(this, vertex, newFragmentID);
				log.info("Added nwe dummies to " + newFragmentID);
			}


		}

	}



}
