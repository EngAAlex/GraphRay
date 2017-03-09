/**
 * 
 */
package com.graphray;

import java.io.IOException;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.common.vertextypes.PathfinderVertexID;
import com.graphray.common.vertextypes.PathfinderVertexType;
import com.graphray.masters.GraphRayMasterCompute;

/**
 * @author spark
 *
 */
public class GraphRayComputation<I extends Writable, M extends Writable> extends AbstractComputation<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType, I, M> {

	protected static Logger log = Logger.getLogger(GraphRayComputation.class);
	
	protected boolean isLogEnabled = false;
	
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> vertex, Iterable<I> messages)
			throws IOException {
		if(isLogEnabled)
			log.info("im " + vertex.getId());
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.bsp.CentralizedServiceWorker, org.apache.giraph.worker.WorkerGlobalCommUsage)
	 */
	@Override
	public void initialize(GraphState graphState,
			WorkerClientRequestProcessor<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> workerClientRequestProcessor,
			CentralizedServiceWorker<PathfinderVertexID, PathfinderVertexType, PathfinderEdgeType> serviceWorker,
			WorkerGlobalCommUsage workerGlobalCommUsage) {
		super.initialize(graphState, workerClientRequestProcessor, serviceWorker, workerGlobalCommUsage);
		isLogEnabled = getConf().getBoolean(GraphRayMasterCompute.enableLogOption, false);
	}


}
