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
/**
 * 
 */
package com.graphray.common.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;

public class JSONMSTOutputFormat extends TextVertexOutputFormat<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> {

	/* (non-Javadoc)
	 * @see org.apache.giraph.io.formats.TextVertexOutputFormat#createVertexWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public TextVertexOutputFormat<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType>.TextVertexWriter createVertexWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		return new JSONMSTVertexWriter();
	}

	/**
	 * @author spark
	 *
	 */
	protected class JSONMSTVertexWriter extends TextVertexWriterToEachLine {

		/* (non-Javadoc)
		 * @see org.apache.giraph.io.formats.TextVertexOutputFormat.TextVertexWriterToEachLine#convertVertexToLine(org.apache.giraph.graph.Vertex)
		 */
		@Override
		protected Text convertVertexToLine(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex)
				throws IOException {
			return new Text("[" + vertex.getId().get() + ",[" + edgeBundler(vertex.getEdges()) + "]");
		}

	    private String edgeBundler(Iterable<Edge<PathfinderVertexID, PathfinderEdgeType>> edges){
	        String result = "";
	        Iterator<Edge<PathfinderVertexID, PathfinderEdgeType>> it = edges.iterator();
	        while(it.hasNext()){
	        	Edge<PathfinderVertexID, PathfinderEdgeType> edge = it.next();
	          result += "[" + edge.getTargetVertexId().get() + "," + edge.getValue().get() + "]";
	          if(it.hasNext())
	            result += ",";
	        }
	        return result;
	      }
		
	}
	
}
