/**
 * 
 */
package unipg.pathfinder.ghs.computations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.Vertex;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
import unipg.pathfinder.utils.Toolbox;


public class GHSComputations{
	
	protected static Logger log = Logger.getLogger(GHSComputations.class);
	
	
	public static class LOEConnection extends PathfinderComputation<ControlledGHSMessage, ControlledGHSMessage> {
		
		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			PathfinderVertexType vertexValue = vertex.getValue();
			vertexValue.resetLOE();
			Iterator<ControlledGHSMessage> msgs = messages.iterator();					
			if(!msgs.hasNext() && vertex.getValue().isRoot()){
				Toolbox.disarmPathfinderCandidates(vertex);
				vertexValue.resetLOE();
				return;
			}
			PathfinderVertexID vertexId = vertex.getId();
			PathfinderVertexID myLOEDestination = vertexValue.getLoeDestination();
			PathfinderVertexID myfragmentIdentity = vertexValue.getFragmentIdentity();
//			while(msgs.hasNext()){
				ControlledGHSMessage current = msgs.next();
				PathfinderVertexID msgFragmentIdentity = current.getFragmentID();
				PathfinderVertexID msgSender = current.getSenderID();
				if(msgFragmentIdentity.equals(myfragmentIdentity)){ //CONNECT MESSAGE CROSSED THE FRAGMENT BORDER
					sendMessage(myLOEDestination, new ControlledGHSMessage(vertexId, myfragmentIdentity, ControlledGHSMessage.CONNECT_MESSAGE));
					vertex.getEdgeValue(myLOEDestination).setAsBranchEdge();
					vertexValue.addBranch();
					vertexValue.resetLOE();
					Toolbox.armPathfinderCandidates(vertex);
				}else if(myLOEDestination == msgSender){ //CONNECT MESSAGE CROSSED THE FRAGMENT BORDER
					if(!vertexValue.isRoot()){
						vertex.getEdgeValue(msgSender).setAsBranchEdge();//FRAGMENTS AGREE ON THE COMMON EDGE
						vertexValue.addBranch();
						vertexValue.resetLOE();
					}
				}
		}
		
	}
}
