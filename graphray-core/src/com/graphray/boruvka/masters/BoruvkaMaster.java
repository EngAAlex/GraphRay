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
package com.graphray.boruvka.masters;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaFindRemainingPathfinders;
import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateCompletion;
import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateConfirmationCompletion;
import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateConfirmationPing;
import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateConfirmationReply;
import com.graphray.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateSetup;
import com.graphray.ghs.computations.EdgeConnectionRoutine;
import com.graphray.ghs.masters.LOEDiscoveryMaster;
import com.graphray.masters.GraphRayMasterCompute;

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
			if(master.getComputation().equals(BoruvkaFindRemainingPathfinders.class)){ //BoruvkaUnassignedEdgesFinalUpdate
				int remainingFragments = ((IntWritable)master.getAggregatedValue(GraphRayMasterCompute.boruvkaProcedureCompletedAggregator)).get();
				master.getContext().getCounter(GraphRayMasterCompute.counterGroup, GraphRayMasterCompute.remainingFragments)
				.setValue(remainingFragments);
				if(remainingFragments == 1){
//					master.getContext().getCounter(GraphRayMasterCompute.counterGroup, GraphRayMasterCompute.remainingFragments)
//					.setValue(0);
					return true;
				}else{
					master.setAggregatedValue(GraphRayMasterCompute.boruvkaProcedureCompletedAggregator, new IntWritable(0));
				}
			}

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
//				master.getContext().getCounter(GraphRayMasterCompute.counterGroup, GraphRayMasterCompute.computationIncomplete)
//				.setValue(1);				
//				master.haltComputation();
				return true;
			}
		}

		if(counter == 2){
			master.setComputation(BoruvkaRootUpdateSetup.class);
			counter++;
			return false;
		}else if(counter == 3){
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
			counter++;
//			counter = 0;
			return false;
		}
		else if(counter == 7){
		master.setComputation(BoruvkaFindRemainingPathfinders.class);
		counter = 0;
		return false;
	}		
//		else if(counter == 7){
//			master.setComputation(BoruvkaUpdateConnectedFragmentsPing.class);
//			counter++;
//			return false;
//		}
//		else if(counter == 8){
//			master.setComputation(BoruvkaUpdateConnectedFragmentsReply.class);
//			counter++;
//			return false;
//		}else if(counter == 9){
//			master.setComputation(BoruvkaUnassignedEdgesFinalUpdate.class);
//			counter = 0;
//			return false;
//		}

		return true;
	}

}
