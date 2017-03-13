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

/**
 * @author spark
 *
 */
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
			return new Text(src.get() + "\t" + edge.getTargetVertexId().get() + "\t" + edge.getValue().get() + "\t" + PathfinderEdgeType.CODE_STRINGS[edge.getValue().getStatus()]);
		}
		
	}

}