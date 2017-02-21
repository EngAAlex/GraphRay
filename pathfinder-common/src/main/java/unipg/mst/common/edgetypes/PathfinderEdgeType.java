package unipg.mst.common.edgetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

public class PathfinderEdgeType extends DoubleWritable {

	public static final short UNASSIGNED = 0;
	public static final short BRANCH = 1;
	public static final short PATHFINDER_CANDIDATE = 2;
	public static final short INTERFRAGMENT_EDGE = 3;
	public static final short DUMMY = 4;
	public static final short PATHFINDER = 5;
	
	public static final String[] CODE_STRINGS = new String[]{"UNASSIGNED",
															"BRANCH",
															"PATHFINDER_CANDIDATE",
															"INTERFRAGMENT",
															"DUMMY",
															"PATHFINDER"};

	short status;
	
	public PathfinderEdgeType() {
		super();
		status = UNASSIGNED;
	}

	public PathfinderEdgeType(double value) {
		super(value);
		status = UNASSIGNED;
	}
	
	/**
	 * @param string
	 */
	public PathfinderEdgeType(String string) {
		this(Double.parseDouble(string));
	}
	
	public PathfinderEdgeType(short status){
		super();
		this.status = status;
	}
	
	public PathfinderEdgeType(double value, short status){
		super(value);
		this.status = status;
	}
	
	public PathfinderEdgeType copy(){
		return new PathfinderEdgeType(get(), status);
	}

	/**
	 * @return the status
	 */
	public short getStatus() {
		return status;
	}

	public void setStatus(short status){
		this.status = status;
	}
	
	/**
	 * @return the branch
	 */
	public boolean isBranch() {
		return status == BRANCH;
	}

	/**
	 * @param branch the branch to set
	 */
	public void setAsBranchEdge() {
		status = BRANCH;
	}

	/**
	 * @return the interFragmentEdge
	 */
	public boolean isInterFragmentEdge() {
		return status == INTERFRAGMENT_EDGE;
	}

	/**
	 * @param interFragmentEdge the interFragmentEdge to set
	 */
	public void setInterFragmentEdge() {
		status = INTERFRAGMENT_EDGE;
	}

	/**
	 * 
	 */
	public void setAsPathfinderCandidate() {
		status = PATHFINDER_CANDIDATE;
	}
	
	public void consolidatePathfinder(){
		status = PATHFINDER;
	}
	
	/**
	 * @return
	 */
	public boolean isPathfinder() {
		return status == PATHFINDER;
	}
	
	/**
	 * @return the isPathfinderCandidate
	 */
	public boolean isPathfinderCandidate() {
		return status == PATHFINDER_CANDIDATE;
	}
	
	
	/**
	 * 
	 */
	public void revertToUnassigned() {
		status = UNASSIGNED;
	}

	/**
	 * @return
	 */
	public boolean unassigned() {
		return status == UNASSIGNED;
	}

	/**
	 * @return
	 */
	public boolean isDummy() {
		return status == DUMMY;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		status = in.readShort();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeShort(status);
	}
	
}
