/**
 * 
 */
package unipg.mst.common.io;

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

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
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
