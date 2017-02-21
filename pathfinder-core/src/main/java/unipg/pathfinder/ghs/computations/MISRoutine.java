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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.messagetypes.ControlledGHSMessage;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;
import unipg.pathfinder.PathfinderComputation;
import unipg.pathfinder.common.writables.DoubleValueAndShortDepth;
import unipg.pathfinder.utils.Toolbox;

/**
 * @author spark
 *
 */
public class MISRoutine {

	MasterCompute master;
	int counter = 0;

	public MISRoutine(MasterCompute master){
		this.master = master;	
	}

	public boolean compute(){
		if(counter == 0){
			master.setComputation(MISPing.class);
			counter++;
			return false;
		}else if(counter == 1){
			master.setComputation(MISReply.class);
			counter++;
			return false;
		}else if(counter == 2){
			master.setComputation(FragmentReconstructionPing.class);
			counter++;
			return false;
		}else if(counter == 3){
				master.setComputation(FragmentReconstructionReply.class);
				counter++;
				return false;
		}else if(counter == 4){
			master.setComputation(FragmentReconstructionCompletion.class);
			counter = 0;			
		}	
		return true;
	}

	public static class MISPing extends PathfinderComputation<Writable, DoubleValueAndShortDepth>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<Writable> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
//			if(vertexValue.isIsolated()){
//				vertexValue.setRoot(true);
//				return;
//			}

			log.info("Starting MIS procedure. My depth " + vertexValue.getDepth());

			if(vertexValue.getDepth() == 1){
				vertexValue.setRoot(true);
			}else
				vertexValue.setRoot(false);

			if(vertexValue.getDepth() == -1){ 
				vertexValue.setMISValue(Math.random());
				log.info("MIS Ping");
				Iterable<PathfinderVertexID> targets = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH, PathfinderEdgeType.INTERFRAGMENT_EDGE);
				if(targets != null)
					sendMessageToMultipleEdges(targets.iterator(), new DoubleValueAndShortDepth(vertexValue.getMISValue(), vertexValue.getDepth()));
			}
			//			else if(vertexValue.getDepth() == 1)
			//				vertexValue.setRoot(true);
		}		
	}

	public static class MISReply extends PathfinderComputation<DoubleValueAndShortDepth, DoubleWritable>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<DoubleValueAndShortDepth> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			int vertexDepth = vertexValue.getDepth();
			if(vertexDepth != -1 /*|| vertexValue.isIsolated()*/)
				return;							
			boolean foundSmaller = false;
			double myValue = vertexValue.getMISValue();
			Iterator<DoubleValueAndShortDepth> it = messages.iterator();			
			while(it.hasNext()){
				if(it.next().getDoubleValue() < myValue){
					foundSmaller = true;
					break;
				}
			}
			log.info("MIS reply " + (!foundSmaller));
			vertexValue.setRoot(!foundSmaller);
		}	
	}

	public static class FragmentReconstructionPing extends PathfinderComputation<DoubleWritable, PathfinderVertexID>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@SuppressWarnings("rawtypes")
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<DoubleWritable> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			vertexValue.setBranches(0);
			if(vertexValue.isRoot()){
				PathfinderVertexID fragmentIdentity = vertex.getId();
				vertexValue.setFragmentIdentity(fragmentIdentity);
				Iterable<PathfinderVertexID> targets = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH, PathfinderEdgeType.INTERFRAGMENT_EDGE);
				log.info("I am the new root");
				if(targets != null){	
					//					vertexValue.setBranches(((HashSet)targets).size());
					sendMessageToMultipleEdges(targets.iterator(), fragmentIdentity.copy());
				}
			}		
		}	
	}

	public static class FragmentReconstructionReply extends PathfinderComputation<PathfinderVertexID, PathfinderVertexID>{

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<PathfinderVertexID> messages) throws IOException {
			super.compute(vertex, messages);
			PathfinderVertexType vertexValue = vertex.getValue();
			if(vertexValue.getFragmentIdentity() != null && !vertexValue.isRoot()){
				reconstructFragment(vertex, vertexValue.getFragmentIdentity());
				return;
			}
			Iterator<PathfinderVertexID> msgs = messages.iterator();
			PathfinderVertexID selectedIdentity = null;
			while(msgs.hasNext()){
				PathfinderVertexID newIdentity = msgs.next().copy();
				if(vertexValue.isRoot()){
					Toolbox.setEdgeAsInterFragmentEdge(vertex, newIdentity);
				}else{
					if(vertexValue.getFragmentIdentity() == null || vertexValue.getFragmentIdentity().get() < newIdentity.get()){
						selectedIdentity = newIdentity;
					}
				}
			}
			if(selectedIdentity != null)
				reconstructFragment(vertex, selectedIdentity);
		}
		
		private void reconstructFragment(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, PathfinderVertexID newIdentity){
			PathfinderVertexType vertexValue = vertex.getValue();
			log.info("Received new fragment identity " + newIdentity);						
			vertexValue.setFragmentIdentity(newIdentity);
			vertexValue.resetDepth();
			Toolbox.setMultipleEdgesAsInterfragment(vertex, Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH));
			Toolbox.setEdgeAsBranch(vertex, newIdentity);
			sendMessage(newIdentity, new PathfinderVertexID(vertex.getId()));
		}
	}
	
	public static class FragmentReconstructionCompletion extends PathfinderComputation<PathfinderVertexID, ControlledGHSMessage>{
		
		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<PathfinderVertexID> messages) throws IOException {
			super.compute(vertex, messages);
			if(!vertex.getValue().isRoot())
				return;
			Iterator<PathfinderVertexID> msgs = messages.iterator();
			Collection<PathfinderVertexID> originalBranches = Toolbox.getSpecificEdgesForVertex(vertex, PathfinderEdgeType.BRANCH);
			HashSet<PathfinderVertexID> newBranches = null;
			while(msgs.hasNext()){
				if(newBranches == null)
					newBranches = new HashSet<PathfinderVertexID>();
				PathfinderVertexID current = msgs.next().copy();
				newBranches.add(current);
				Toolbox.setEdgeAsBranch(vertex, current);				
			}
			if(newBranches != null)
				originalBranches.removeAll(newBranches);
			if(originalBranches != null)
				Toolbox.setMultipleEdgesAsInterfragment(vertex, originalBranches);
			
		}
		
	}

}
