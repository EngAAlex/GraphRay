/**
 * 
 */
package unipg.mst.common.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.mst.common.vertextypes.PathfinderVertexID;
import unipg.mst.common.vertextypes.PathfinderVertexType;

/**
 * @author spark
 *
 */
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
