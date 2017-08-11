/*******************************************************************************
 * Copyright 2017 Alessio Arleo
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package com.graphray;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockApiHandle;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;

import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.messagetypes.ControlledGHSMessage;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;
import com.graphray.masters.GraphRayMasterCompute;

public class DummyComputations {

	/**
	 * @author spark
	 *
	 */
	public static class DummyEdgesCleanupComputation extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage> {

		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();
//			super.compute(vertex, messages);
			while(edges.hasNext()){
				Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
				if(!current.getValue().isBranch() && !current.getValue().isPathfinder()){
//				log.info("Removing Edge from " + vertex.getId() + " to " + current.getTargetVertexId() + " " + PathfinderEdgeType.CODE_STRINGS[current.getValue().getStatus()]);					
					removeEdgesRequest(vertex.getId(), current.getTargetVertexId().copy());
				}
//				else if(current.getValue().isPathfinder())
//					getContext().getCounter(MSTPathfinderMasterCompute.counterGroup, MSTPathfinderMasterCompute.pathfinderCounter).increment(1);
			}
		}
	}
	
	public static class NOOPComputation extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage>{
		
		/* (non-Javadoc)
		 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
				Iterable<ControlledGHSMessage> messages) throws IOException {
			super.compute(vertex, messages);
//			Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> edges = vertex.getEdges().iterator();			
//			while(edges.hasNext()){
//				Edge<PathfinderVertexID, PathfinderEdgeType> current = edges.next();
////				log.info("Final sweep Edge " + current.getTargetVertexId() + " " + PathfinderEdgeType.CODE_STRINGS[current.getValue().getStatus()]);
//			}
			vertex.voteToHalt();
		}
		
	}

}
