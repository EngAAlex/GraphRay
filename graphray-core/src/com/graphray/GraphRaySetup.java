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

import org.apache.giraph.graph.Vertex;

import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.messagetypes.ControlledGHSMessage;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
public class GraphRaySetup extends GraphRayComputation<ControlledGHSMessage, ControlledGHSMessage> {

	/* (non-Javadoc)
	 * @see unipg.pathfinder.PathfinderComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex,
			Iterable<ControlledGHSMessage> messages) throws IOException {
		super.compute(vertex, messages);
		vertex.getValue().setFragmentIdentity(vertex.getId().copy());
		if(isLogEnabled)
			log.info(" setting fragment " + vertex.getValue().getFragmentIdentity());
		if(vertex.getNumEdges() == 0){
			vertex.getValue().loesDepleted();
			vertex.voteToHalt();
		}
	}
	
}
