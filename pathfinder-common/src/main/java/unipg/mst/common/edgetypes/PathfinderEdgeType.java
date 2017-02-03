package unipg.mst.common.edgetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

public class PathfinderEdgeType extends DoubleWritable {

	boolean branch = false;
	boolean pathfinder = false;
	boolean isPathfinderCandidate = false;
	
	public PathfinderEdgeType() {
		super();
	}

	public PathfinderEdgeType(double value) {
		super(value);
	}
	
//	public PathfinderEdgeType(double value, boolean bool){
//		this(value);
//		isPathfinderCandidate = bool;
//	}
//	
	/**
	 * @return the branch
	 */
	public boolean isBranch() {
		return branch;
	}

	/**
	 * @param branch the branch to set
	 */
	public void setAsBranchEdge() {
		branch = true;
	}

	/**
	 * 
	 */
	public void setAsPathfinderCandidate() {
		isPathfinderCandidate = true;
	}
	
	/**
	 * @return the isPathfinderCandidate
	 */
	public boolean isPathfinderCandidate() {
		return isPathfinderCandidate;
	}

	/**
	 * @return the pathfinder
	 */
	public boolean isPathfinder() {
		return pathfinder;
	}

	/**
	 * @param pathfinder the pathfinder to set
	 */
	public void setPathfinder(boolean pathfinder) {
		this.pathfinder = pathfinder;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		isPathfinderCandidate = in.readBoolean();
		branch = in.readBoolean();
		pathfinder = in.readBoolean();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeBoolean(isPathfinderCandidate);
		out.writeBoolean(branch);
		out.writeBoolean(pathfinder);
	}
	
}
