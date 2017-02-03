/**
 * 
 */
package pathfinder.utils.algorithms.mis;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class MISPieces {

	/**
	 * @author spark
	 *
	 */
	public static class MISComputation<S extends LongWritable, T extends BooleanWritable> extends Piece<S, T, Writable, LongWritable, Object> {		

		/* (non-Javadoc)
		 * @see org.apache.giraph.block_app.framework.piece.DefaultParentPiece#getVertexSender(org.apache.giraph.block_app.framework.api.BlockWorkerSendApi, java.lang.Object)
		 */
		@Override
		public VertexSender<S, T, Writable> getVertexSender(
				BlockWorkerSendApi<S, T, Writable, LongWritable> workerApi, Object executionStage) {
			return (vertex) -> {
				long id = vertex.getId().get();
				workerApi.sendMessageToAllEdges(vertex, new LongWritable(id));
			}; 
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.block_app.framework.piece.AbstractPiece#getVertexReceiver(org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi, java.lang.Object)
		 */
		@Override	
		public VertexReceiver<S, T, Writable, LongWritable> getVertexReceiver(
				BlockWorkerReceiveApi<S> workerApi, Object executionStage) {
			return (vertex, messages) -> {
				boolean foundSmaller = false;
				long id = vertex.getId().get();
				Iterator<LongWritable> it = messages.iterator();			
				while(it.hasNext()){
					if(it.next().get() < id){
						foundSmaller = true;
						break;
					}
				}
				vertex.getValue().set(!foundSmaller);
			};
		}
	}
	
//	public static class MISReset<S extends LongWritable, T extends BooleanWritable> extends Piece<S, T, Writable, DoubleWritable, Object>{
//		/* (non-Javadoc)
//		 * @see org.apache.giraph.block_app.framework.piece.DefaultParentPiece#getVertexSender(org.apache.giraph.block_app.framework.api.BlockWorkerSendApi, java.lang.Object)
//		 */
//		@Override
//		public VertexSender<S, T, Writable> getVertexSender(
//				BlockWorkerSendApi<S, T, Writable, DoubleWritable> workerApi, Object executionStage) {
//			return (vertex) -> {
//			};
//		}		
//	}
}
