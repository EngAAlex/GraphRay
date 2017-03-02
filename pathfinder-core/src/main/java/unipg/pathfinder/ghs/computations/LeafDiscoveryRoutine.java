/**
 * 
 */
package unipg.pathfinder.ghs.computations;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;


import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexID.PathfinderVertexIDWithShortValue;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
import unipg.pathfinder.utils.Toolbox;

/**
 * @author spark
 *
 */
public class LeafDiscoveryRoutine {
	
	MasterCompute master;
	int counter = 0;

	public LeafDiscoveryRoutine(MasterCompute master){
		this.master = master;	
	}

	public boolean compute(){
		if(counter == 0 ){
			master.setComputation(LeafDiscoveryFirstPing.class);
			counter++;
			return false;
		}else if(counter == 1){
			master.setComputation(LeafDiscoveryReply.class);
			counter++;
			return false;
		}else if(counter == 2){
			master.setComputation(LeafDiscoverySecondPing.class);	
			counter++;
			return false;
		}else if(counter == 3){
			counter++;
			master.setComputation(LeafDiscoveryReply.class);
			return false;
		}else if(counter == 4)
			counter = 0;
		return true;
	}


	public static class LeafDiscoveryFirstPing extends PathfinderComputation<Writable, PathfinderVertexIDWithShortValue> {

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<Writable> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();

			vertexValue.setFragmentIdentity(null);
			
//			log.info("I have " + vertexValue.noOfBranches() + " branches");
			
			vertexValue.resetDepth();

//			if(vertexValue.isIsolated()){
////				vertexValue.setDepth((short) 1);
//				return;
//			}

			Collection<PathfinderVertexID> destinations = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH/*, PathfinderEdgeType.INTERFRAGMENT_EDGE*/);				
			//			if(vertexValue.getDepth() == -1){
			if(destinations != null && destinations.size() == 1){
				log.info("sending leaf message");
				vertexValue.setDepth((short) 0);
				sendMessageToMultipleEdges(destinations.iterator(), new PathfinderVertexIDWithShortValue(vertex.getId(), (short) 0));
				//				}else 
				//					sendMessageToMultipleEdges(destinations, new PathfinderVertexIDWithByte(vertex.getId().copy(), (byte) 1));
			}else
				return;
		}		
	}

	public static class LeafDiscoverySecondPing extends PathfinderComputation<Writable, PathfinderVertexIDWithShortValue> {

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<Writable> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
//			if(vertexValue.isIsolated())
//				return;

			if(vertexValue.getDepth() == 1){
				log.info("sending depth 1 message");
				//					vertexValue.setDepth((byte) 0);
				//					sendMessageToMultipleEdges(destinations, new PathfinderVertexIDWithByte(vertex.getId().copy(), (byte) 0));
				//				}else 
				sendMessageToMultipleEdges(Toolbox.getSpecificEdgesForVertex(
						vertex, PathfinderEdgeType.BRANCH/*, PathfinderEdgeType.INTERFRAGMENT_EDGE*/).iterator(), new PathfinderVertexIDWithShortValue(vertex.getId(), (short) 1));
			}else
				return;
		}		
	}

	public static class LeafDiscoveryReply extends PathfinderComputation<PathfinderVertexIDWithShortValue, PathfinderVertexIDWithShortValue>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<PathfinderVertexIDWithShortValue> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			short depth = vertexValue.getDepth();
			Iterator<PathfinderVertexIDWithShortValue> msgs = messages.iterator();
			while(msgs.hasNext()){							
				PathfinderVertexIDWithShortValue message = msgs.next();
				log.info("Received leaf message " + message.getPfID() + " value " + message.getDepth());
				switch(message.getDepth()){
				case 0:
					if(depth == -1){
						log.info("setting depth to 1");		
						vertexValue.setDepth((short) 1);
					}else if(depth == 0){
						if(vertex.getId().get() > message.get())
							vertexValue.setDepth((byte) 1);
					}
				break;
				case (byte) 1:
					if(depth == -1){
						log.info("setting depth to 2");
						vertexValue.setDepth((short) 2);
						vertexValue.setFragmentIdentity(message.getPfID());
					}
				break;
				}
			}
		}	
	}

}
