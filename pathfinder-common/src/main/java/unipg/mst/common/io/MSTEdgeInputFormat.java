/**
 * 
 */
package unipg.mst.common.io;

import java.io.IOException;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;

/**
 * @author spark
 *
 */
public class MSTEdgeInputFormat extends TextEdgeInputFormat<PathfinderVertexID, PathfinderEdgeType> {

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
