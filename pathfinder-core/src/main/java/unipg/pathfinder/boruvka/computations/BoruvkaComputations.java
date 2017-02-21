/**
 * 
 */
package unipg.pathfinder.boruvka.computations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
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
			vertex.getValue().resetLOE();
			vertex.getValue().resetLOEDepleted();
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

	public static class BoruvkaRootUpdatePing extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.isRoot() && !vertexValue.boruvkaStatus()){ //the vertex is a deactivated root and must be updated
				vertexValue.setRoot(false); //the vertex will remain silent from now on
				log.info("Deactivating for Boruvka");
				sendMessageToMultipleEdges(Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH, PathfinderEdgeType.DUMMY).iterator(), 
						new ControlledGHSMessage(vertex.getId(), vertexValue.getFragmentIdentity(), ControlledGHSMessage.ROOT_UPDATE));
				if(!vertexValue.hasLOEsDepleted()){
					vertexValue.reactivateForBoruvka();
					if(vertex.getEdgeValue(vertexValue.getFragmentIdentity()) == null){
						addEdgeRequest(vertex.getId(), EdgeFactory.create(vertexValue.getFragmentIdentity(), new PathfinderEdgeType(PathfinderEdgeType.DUMMY))); //new pair is created
						addEdgeRequest(vertexValue.getFragmentIdentity(), EdgeFactory.create(vertex.getId(), new PathfinderEdgeType(PathfinderEdgeType.DUMMY)));
					}
					log.info("Reactivating for Boruvka, LOES not depleted");

				}
			}
		}		
	}

	public static class BoruvkaRootUpdateReply extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			PathfinderVertexType vertexValue = vertex.getValue();
			if(!vertexValue.boruvkaStatus() || vertex.getValue().isRoot() || vertex.getValue().hasLOEsDepleted())
				return;
			super.compute(vertex, messages);

			Iterator<ControlledGHSMessage> msgs = messages.iterator();
			PathfinderVertexID newFragmentID = msgs.next().getFragmentID();
			log.info("Updating with new fragment " + newFragmentID);
			Iterable<PathfinderVertexID> dummyEdges = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.DUMMY);
			if(dummyEdges != null){
				Iterator<PathfinderVertexID> dummyEdgesIt = dummyEdges.iterator();
				while(dummyEdgesIt.hasNext()){
					PathfinderVertexID currentDummy = dummyEdgesIt.next();				//old pair of dummies are removed
					removeEdgesRequest(vertex.getId(), currentDummy);
					removeEdgesRequest(currentDummy, vertex.getId());
				}
			}
			//			if(msgs.hasNext()) what if is not the only message?
			//				throw new Exception();
			vertexValue.setFragmentIdentity(newFragmentID);
			addEdgeRequest(vertex.getId(), EdgeFactory.create(newFragmentID, new PathfinderEdgeType(PathfinderEdgeType.DUMMY))); //new pair is created
			addEdgeRequest(newFragmentID, EdgeFactory.create(vertex.getId(), new PathfinderEdgeType(PathfinderEdgeType.DUMMY)));

		}

	}



}
