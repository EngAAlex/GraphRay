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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextEdgeOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;

public class GraphRayEdgeOutputFormat extends TextEdgeOutputFormat<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> {

	/* (non-Javadoc)
	 * @see org.apache.giraph.io.formats.TextEdgeOutputFormat#createEdgeWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public TextEdgeWriter createEdgeWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		return new MSTEdgeWriter();
	}
	
	public class MSTEdgeWriter extends TextEdgeWriterToEachLine<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType>{

		/* (non-Javadoc)
		 * @see org.apache.giraph.io.formats.TextEdgeOutputFormat.TextEdgeWriterToEachLine#convertEdgeToLine(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.Writable, org.apache.giraph.edge.Edge)
		 */
		@Override
		protected Text convertEdgeToLine(PathfinderVertexID src, PathfinderVertexType srcValue,
				Edge<PathfinderVertexID, PathfinderEdgeType> edge) throws IOException {
			if(edge.getValue().isBranch() || edge.getValue().isPathfinder())
				return new Text(src.get() + "\t" + edge.getTargetVertexId().get() + "\t" + edge.getValue().get() + "\t" + PathfinderEdgeType.CODE_STRINGS[edge.getValue().getStatus()]);
			else
				return new Text("");
		}
		
	}

}
