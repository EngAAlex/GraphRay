/**
 * 
 */
package unipg.pathfinder.masters;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.master.DefaultMasterCompute;

import unipg.pathfinder.boruvka.masters.BoruvkaMaster;
import unipg.pathfinder.ghs.computations.LOEDiscovery;
import unipg.pathfinder.ghs.masters.GHSMaster;

/**
 * @author spark
 *
 */
public class MSTPathfinderMasterCompute extends DefaultMasterCompute {

	public static final String cGHSProcedureCompletedAggregator = "AGG_COMPLETE_GHS";
	public static final String boruvkaProcedureCompletedAggregator = "AGG_COMPLETE_BORUVKA";
	
	public static final String controllerGHSExecution = "CONTROLLED_GHS";
	
	GHSMaster ghs;
	LOEDiscovery lD;
	BoruvkaMaster boruvka;
	
	short stage = 0;

	/* (non-Javadoc)
	 * @see org.apache.giraph.master.MasterCompute#compute()
	 */
	@Override
	public void compute() {
		if(stage == 0)
			if(ghs.compute())
				stage = 1;
			else
				return;
		if(stage == 1)
			if(boruvka.compute())
				haltComputation();
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.master.MasterCompute#initialize()
	 */
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		registerAggregator(cGHSProcedureCompletedAggregator, BooleanAndAggregator.class);
		registerAggregator(boruvkaProcedureCompletedAggregator, IntSumAggregator.class);
		lD = new LOEDiscovery(this);
		ghs = new GHSMaster(this, lD);
		boruvka = new BoruvkaMaster(this, lD);				
	}

}
