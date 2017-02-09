/**
 * 
 */
package unipg.mst.common.vertextypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author spark
 *
 */
public class PathfinderVertexType extends DoubleWritable { //MISValue

	protected boolean isRoot;
	protected boolean boruvkaStatus;
	protected PathfinderVertexID fragmentIdentity;
	protected double loeValue;
	protected PathfinderVertexID loeDestination;
	protected int branches;
	protected byte depth;
	protected boolean loesDepleted;
	
	/**
	 * 
	 */
	public PathfinderVertexType() {
		super();
		isRoot = true;
		boruvkaStatus = true;
		depth = -1;
		branches = 0;
		loeValue = Double.MAX_VALUE;
	}
	
	

	/**
	 * @return the isRoot
	 */
	public boolean isRoot() {
		return isRoot;
	}

	/**
	 * @param isRoot the isRoot to set
	 */
	public void setRoot(boolean isRoot) {
		this.isRoot = isRoot;
	}
	
	/**
	 * @return
	 */
	public double getMISValue(){
		return super.get();
	}
	
	/**
	 * 
	 */
	public void setMISValue(double value){
		super.set(value);
	}

	/**
	 * @return the depth
	 */
	public int getDepth() {
		return depth;
	}

	/**
	 * @param depth the depth to set
	 */
	public void setDepth(byte depth) {
		this.depth = depth;
	}
	
	public void resetDepth(){
		depth = -1;
	}

	/**
	 * @return the loe
	 */
	public double getLOE() {
		return loeValue;
	}



	/**
	 * @param loe the loe to set
	 */
	public void updateLOE(double loe) {
		this.loeValue = loe;
	}
	
	/**
	 * 
	 */
	public void resetLOE(){
		updateLOE(Double.MAX_VALUE);
		setLoeDestination(null);
	}



	/**
	 * @return the loeDestination
	 */
	public PathfinderVertexID getLoeDestination() {
		return loeDestination;
	}

	public void addBranch(){
		branches++;
	}
	
	public boolean isLeaf(){
		return branches == 1;
	}
	
	public boolean isIsolated(){
		return branches == 0;
	}

	/**
	 * @return the edgeToRoot
	 */
	public PathfinderVertexID getFragmentIdentity() {
		return fragmentIdentity;
	}



	/**
	 * @param edgeToRoot the edgeToRoot to set
	 */
	public void setFragmentIdentity(PathfinderVertexID fragmentIdentity) {
		this.fragmentIdentity = fragmentIdentity;
	}



	/**
	 * @param loeDestination the loeDestination to set
	 */
	public void setLoeDestination(PathfinderVertexID loeDestination) {
		this.loeDestination = loeDestination;
	}



	/**
	 * @return the loesDepleted
	 */
	public boolean hasLOEsDepleted() {
		return loesDepleted;
	}



	/**
	 * @param loesDepleted the loesDepleted to set
	 */
	public void loesDepleted() {
		this.loesDepleted = false;
	}



	/**
	 * @return the boruvkaStatus
	 */
	public boolean boruvkaStatus() {
		return boruvkaStatus;
	}



	/**
	 * @param boruvkaStatus the boruvkaStatus to set
	 */
	public void setInactive() {
		this.boruvkaStatus = false;
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		isRoot = in.readBoolean();
		boruvkaStatus = in.readBoolean();
		depth = in.readByte();
		loeValue = in.readDouble();
		loeDestination.readFields(in);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeBoolean(isRoot);
		out.writeBoolean(boruvkaStatus);
		out.writeByte(depth);
		out.writeDouble(loeValue);
		loeDestination.write(out);
	}


}
