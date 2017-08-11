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
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;
import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;

public class JSONMSTInputFormat extends TextVertexInputFormat<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> {

	/* (non-Javadoc)
	 * @see org.apache.giraph.io.formats.TextVertexInputFormat#createVertexReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public TextVertexInputFormat<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType>.TextVertexReader createVertexReader(
			InputSplit is, TaskAttemptContext tac) throws IOException {
		return new JSONMSTVertexReader();
	}
	
	/**
	 * @author spark
	 *
	 */
	protected class JSONMSTVertexReader extends TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {

		/* (non-Javadoc)
		 * @see org.apache.giraph.io.formats.TextVertexInputFormat.TextVertexReaderFromEachLineProcessedHandlingExceptions#getId(java.lang.Object)
		 */
		@Override
		protected PathfinderVertexID getId(JSONArray jarr) throws JSONException, IOException {
			return new PathfinderVertexID(jarr.getLong(0));
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.io.formats.TextVertexInputFormat.TextVertexReaderFromEachLineProcessedHandlingExceptions#getValue(java.lang.Object)
		 */
		@Override
		protected PathfinderVertexType getValue(JSONArray jarr) throws JSONException, IOException {
			return new PathfinderVertexType();
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.io.formats.TextVertexInputFormat.TextVertexReaderFromEachLineProcessedHandlingExceptions#preprocessLine(org.apache.hadoop.io.Text)
		 */
		@Override
		protected JSONArray preprocessLine(Text txt) throws JSONException, IOException {
			return new JSONArray(txt.toString());
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.io.formats.TextVertexInputFormat.TextVertexReaderFromEachLineProcessedHandlingExceptions#getEdges(java.lang.Object)
		 */
		@Override
		protected Iterable<Edge<PathfinderVertexID, PathfinderEdgeType>> getEdges(JSONArray jsonVertex)
				throws JSONException, IOException {
			JSONArray jsonEdgeArray = jsonVertex.getJSONArray(1);
			List<Edge<PathfinderVertexID, PathfinderEdgeType>> edges =	Lists.newArrayList();
			int i;
			for (i = 0; i < jsonEdgeArray.length(); ++i) {
				JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
				edges.add(EdgeFactory.create(new PathfinderVertexID(jsonEdge.getLong(0)),
						new PathfinderEdgeType(jsonEdge.getDouble(1))));
			}
			return edges;
		}		
		
	}
	
	

}
