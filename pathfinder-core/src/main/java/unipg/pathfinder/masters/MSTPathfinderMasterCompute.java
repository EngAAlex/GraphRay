/**
 * 
 */
package unipg.pathfinder.masters;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * @author spark
 *
 */
public class MSTPathfinderMasterCompute extends DefaultMasterCompute {

	public static final String procedureCompletedAggregator = "AGG_COMPLETE_GHS";
	public static final String controllerGHSExecution = "CONTROLLED_GHS";
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.master.MasterCompute#compute()
	 */
	@Override
	public void compute() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.master.MasterCompute#initialize()
	 */
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		registerAggregator(procedureCompletedAggregator, BooleanAndAggregator.class);
	}

}
