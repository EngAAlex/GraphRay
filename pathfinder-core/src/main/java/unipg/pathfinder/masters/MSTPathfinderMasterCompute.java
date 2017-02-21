/**
 * 
 */
package unipg.pathfinder.masters;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.pathfinder.DummyComputations.DummyEdgesCleanupComputation;
import unipg.pathfinder.DummyComputations.NOOPComputation;
import unipg.pathfinder.boruvka.masters.BoruvkaMaster;
import unipg.pathfinder.ghs.masters.GHSMaster;
import unipg.pathfinder.ghs.masters.LOEDiscoveryMaster;

/**
 * @author spark
 *
 */
public class MSTPathfinderMasterCompute extends DefaultMasterCompute {

	public static final String cGHSProcedureCompletedAggregator = "AGG_COMPLETE_GHS";
	public static final String boruvkaProcedureCompletedAggregator = "AGG_COMPLETE_BORUVKA";
	
	public static final String messagesLeftAggregator = "AGG_MSGS_LEFT";
	
	public static final String controllerGHSExecution = "CONTROLLED_GHS";
	public static final String loesToDiscoverAggregator = "CURRENT_LOES";
	
	GHSMaster ghs;
	LOEDiscoveryMaster lD;
	BoruvkaMaster boruvka;
	
	short stage = 0;

	/* (non-Javadoc)
	 * @see org.apache.giraph.master.MasterCompute#compute()
	 */
	@Override
	public void compute() {
		if(getSuperstep() == 0)
			return;		
		
		if(stage == 0){
			if(ghs.compute()){
				boruvka.compute();
				stage = 1;
			}else
				return;
		}else if(stage == 1){
			if(boruvka.compute()){
				setComputation(DummyEdgesCleanupComputation.class);
				stage = 2;
				return;
			}
		}else
			setComputation(NOOPComputation.class);
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.master.MasterCompute#initialize()
	 */
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		registerAggregator(messagesLeftAggregator, BooleanAndAggregator.class);
		registerAggregator(cGHSProcedureCompletedAggregator, BooleanAndAggregator.class);		
		
		registerPersistentAggregator(boruvkaProcedureCompletedAggregator, IntSumAggregator.class);		
		registerPersistentAggregator(loesToDiscoverAggregator, IntMaxAggregator.class);
		setAggregatedValue(loesToDiscoverAggregator, new IntWritable(PathfinderEdgeType.UNASSIGNED));
		
		lD = new LOEDiscoveryMaster(this);
		ghs = new GHSMaster(this, lD);
		boruvka = new BoruvkaMaster(this, lD);				
	}

}
