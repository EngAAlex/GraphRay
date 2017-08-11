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

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.vertextypes.PathfinderVertexID;

public class GraphRayEdgeInputFormat extends TextEdgeInputFormat<PathfinderVertexID, PathfinderEdgeType> {

	/* (non-Javadoc)
	 * @see org.apache.giraph.io.EdgeInputFormat#createEdgeReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public EdgeReader<PathfinderVertexID, PathfinderEdgeType> createEdgeReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException {
		return new MSTEdgeInputEdgeReader();
	}
	
	public class MSTEdgeInputEdgeReader extends TextEdgeReaderFromEachLine{

		/* (non-Javadoc)
		 * @see org.apache.giraph.io.formats.TextEdgeInputFormat.TextEdgeReaderFromEachLine#getSourceVertexId(org.apache.hadoop.io.Text)
		 */
		@Override
		protected PathfinderVertexID getSourceVertexId(Text line) throws IOException {
			return new PathfinderVertexID(line.toString().split("\t")[0]);
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.io.formats.TextEdgeInputFormat.TextEdgeReaderFromEachLine#getTargetVertexId(org.apache.hadoop.io.Text)
		 */
		@Override
		protected PathfinderVertexID getTargetVertexId(Text line) throws IOException {
			return new PathfinderVertexID(line.toString().split("\t")[1]);
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.io.formats.TextEdgeInputFormat.TextEdgeReaderFromEachLine#getValue(org.apache.hadoop.io.Text)
		 */
		@Override
		protected PathfinderEdgeType getValue(Text line) throws IOException {
			return new PathfinderEdgeType(line.toString().split("\t")[2]);
		}
		
	}

}
