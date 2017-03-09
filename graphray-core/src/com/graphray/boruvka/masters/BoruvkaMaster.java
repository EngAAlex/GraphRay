/**
 * 
 */
package com.graphray.boruvka.masters;

import java.io.IOException;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateCompletion;
import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateConfirmationCompletion;
import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateConfirmationPing;
import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateConfirmationReply;
import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateSetup;
import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.ghs.computations.EdgeConnectionRoutine;
import com.graphray.ghs.masters.LOEDiscoveryMaster;
import com.graphray.masters.GraphRayMasterCompute;

/**
 * @author spark
 *
 */
public class BoruvkaMaster {

	protected static Logger log = Logger.getLogger(BoruvkaMaster.class);

	MasterCompute master;
	LOEDiscoveryMaster lD;
	EdgeConnectionRoutine ecr;
	int counter;

	/**
	 * 
	 */
	public BoruvkaMaster(MasterCompute master, LOEDiscoveryMaster lD, EdgeConnectionRoutine ecr) {
		this.master = master;
		this.lD = lD;
		this.ecr = ecr;
		counter = 0;
	}

	/**
	 * 
	 */
	public boolean compute() {				
		if(counter == 0){
			if(lD.compute()){
				counter++;
				try {
					ecr.compute();
				} catch (Exception e) {
					e.printStackTrace();
					master.getContext().getCounter(GraphRayMasterCompute.counterGroup, GraphRayMasterCompute.computationIncomplete)
						.setValue(1);					
					master.haltComputation();
				}
			}
			return false;
		}else if(counter == 1){

			try {
				if(ecr.compute())
					counter++;
				else
					return false;
			} catch (Exception e) {
				e.printStackTrace();
				master.getContext().getCounter(GraphRayMasterCompute.counterGroup, GraphRayMasterCompute.computationIncomplete)
					.setValue(1);				
				master.haltComputation();
			}
		}
		
		if(counter == 2){
			master.setComputation(BoruvkaRootUpdateSetup.class);
			counter++;
			return false;
		}else if(counter == 3){
			if(((IntWritable)master.getAggregatedValue(GraphRayMasterCompute.boruvkaProcedureCompletedAggregator)).get() == 1){
				master.getContext().getCounter(GraphRayMasterCompute.counterGroup, GraphRayMasterCompute.remainingFragments)
					.setValue(0);				
				return true;
			}else{
				master.getContext().getCounter(GraphRayMasterCompute.counterGroup, GraphRayMasterCompute.remainingFragments)
					.setValue(((IntWritable)master.getAggregatedValue(GraphRayMasterCompute.boruvkaProcedureCompletedAggregator)).get());
				master.setAggregatedValue(GraphRayMasterCompute.boruvkaProcedureCompletedAggregator, new IntWritable(0));
			}
			master.getContext().getCounter(GraphRayMasterCompute.counterGroup, GraphRayMasterCompute.boruvkaRounds).increment(1);
			master.setComputation(BoruvkaRootUpdateConfirmationPing.class);
			counter++;
			return false;
		}else if(counter == 4){
			master.setComputation(BoruvkaRootUpdateConfirmationReply.class);
			counter++;
			return false;
		}else if(counter == 5){
			master.setComputation(BoruvkaRootUpdateConfirmationCompletion.class);
			counter++;
			return false;
		}else if(counter == 6){
			master.setComputation(BoruvkaRootUpdateCompletion.class);
			counter = 0;
			return false;
		}

		return true;
	}

}
