/**
 * 
 */
package unipg.pathfinder.masters;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * @author spark
 *
 */
public class MSTPathfinderMasterCompute extends DefaultMasterCompute {

	public static final String cGHSProcedureCompletedAggregator = "AGG_COMPLETE_GHS";
	public static final String boruvkaProcedureCompletedAggregator = "AGG_COMPLETE_BORUVKA";
	
	public static final String controllerGHSExecution = "CONTROLLED_GHS";

	/* (non-Javadoc)
	 * @see org.apache.giraph.master.MasterCompute#compute()
	 */
	@Override
	public void compute() {
		
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.master.MasterCompute#initialize()
	 */
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		registerAggregator(cGHSProcedureCompletedAggregator, BooleanAndAggregator.class);
		registerAggregator(boruvkaProcedureCompletedAggregator, IntSumAggregator.class);
	}

}
