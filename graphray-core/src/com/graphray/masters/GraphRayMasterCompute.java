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
package com.graphray.masters;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;

import com.graphray.DummyComputations.DummyEdgesCleanupComputation;
import com.graphray.DummyComputations.NOOPComputation;
import com.graphray.boruvka.masters.BoruvkaMaster;
import com.graphray.common.edgetypes.PathfinderEdgeType;
import com.graphray.ghs.computations.EdgeConnectionRoutine;
import com.graphray.ghs.masters.LOEDiscoveryMaster;

public class GraphRayMasterCompute extends DefaultMasterCompute {

	public static final String boruvkaProcedureCompletedAggregator = "AGG_COMPLETE_BORUVKA";
	
	public static final String messagesLeftAggregator = "AGG_MSGS_LEFT";
	
	public static final String branchSkippedAggregator = "BRANCH_SKIPPED";
	public static final String testMessages = "AGG_CONNECTION_MASTER";
	
	public static final String controllerGHSExecution = "CONTROLLED_GHS";
	public static final String loesToDiscoverAggregator = "CURRENT_LOES";
	
	public static final String counterGroup = "MST-Pathfinder counters";
	public static final String boruvkaRounds = "Boruvka rounds";
	public static final String finalEdges = "Edgeset size";
	public static final String remainingFragments = "Remaining fragments";
	public static final String computationIncomplete = "Computation Incomplete";
	
	public static final String enableLogOption = "graphray.enableLogging";
	
	LOEDiscoveryMaster lD;
	EdgeConnectionRoutine ecr;
	BoruvkaMaster boruvka;
	
	short stage = 0;

	/* (non-Javadoc)
	 * @see org.apache.giraph.master.MasterCompute#compute()
	 */
	@Override
	public void compute() {
		if(getSuperstep() == 0){
			stage = 1;
			return;		
		}
		
		if(stage == 1){
			if(boruvka.compute()){
				haltComputation();
//				setComputation(DummyEdgesCleanupComputation.class);
//				stage = 2;
//				return;
			}
		}
//		else
//			haltComputation();
//			setComputation(NOOPComputation.class);
	}
	
	

	/* (non-Javadoc)
	 * @see org.apache.giraph.master.MasterCompute#initialize()
	 */
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		registerAggregator(messagesLeftAggregator, BooleanAndAggregator.class);
		registerPersistentAggregator(branchSkippedAggregator, BooleanAndAggregator.class);
		
		registerPersistentAggregator(boruvkaProcedureCompletedAggregator, IntSumAggregator.class);		
		registerPersistentAggregator(loesToDiscoverAggregator, IntMaxAggregator.class);
		setAggregatedValue(loesToDiscoverAggregator, new IntWritable(PathfinderEdgeType.UNASSIGNED));
		setAggregatedValue(GraphRayMasterCompute.boruvkaProcedureCompletedAggregator, new IntWritable(0));
				
		getContext().getCounter(GraphRayMasterCompute.counterGroup, GraphRayMasterCompute.boruvkaRounds).increment(0);
		
		lD = new LOEDiscoveryMaster(this);
		ecr = new EdgeConnectionRoutine(this);
		boruvka = new BoruvkaMaster(this, lD, ecr);			
	}

}
