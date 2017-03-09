/**
 * 
 */
package com.graphray.common.vertextypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.graphray.common.writables.SetWritable;

/**
 * @author spark
 *
 */
public class PathfinderVertexType extends DoubleWritable { //MISValue

	protected boolean isRoot;
	protected boolean boruvkaStatus;
	protected PathfinderVertexID fragmentIdentity;
	protected double loeValue;
//	protected PathfinderVertexID loeDestination;
	protected PathfinderVertexID loeDestinationFragment;	
	protected boolean loesDepleted;
	protected MapWritable loeAlternatives;
	protected SetWritable<PathfinderVertexID> acceptedConnections;
	
	protected boolean clearedForConnection;
	protected boolean pingedByRoot;
	protected boolean branchConnection;
	
	/**
	 * 
	 */
	public PathfinderVertexType() {
		super();
		isRoot = true;
		boruvkaStatus = true;
		loeValue = Double.MAX_VALUE;
		fragmentIdentity = new PathfinderVertexID();
		loeDestinationFragment = new PathfinderVertexID();		
		loeAlternatives = new MapWritable();
		acceptedConnections = new SetWritable<PathfinderVertexID>();
	}
	
//	/**
//	 * 
//	 */
//	public PathfinderVertexType(PathfinderVertexID fragmentIdentity) {
//		super();
//		isRoot = true;
//		boruvkaStatus = true;
//		depth = -1;
//		branches = 0;
//		loeValue = Double.MAX_VALUE;
//		this.fragmentIdentity = fragmentIdentity;
//		loeAlternatives = new MapWritable();		
//	}
	

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
//		setLoeDestination(null);
		setLoeDestinationFragment(null);
		clearedForConnection = false;		
		acceptedConnections.clear();
		setPingedByRoot(false);
		branchConnection = false;
	}
	
	public void setLOEStack(MapWritable loeStack){
		loeAlternatives = loeStack;
	}
	
	public Set<Writable> getActiveFragments(){
		return loeAlternatives.keySet();
	}

	public void resetLOEStack(){
		loeAlternatives.clear();;
	}

	/**
	 * @return
	 */
	public int stackSize() {
		return loeAlternatives.size();
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
	
	public void clearAcceptedConnections(){
		acceptedConnections.clear();
	}

	public void acceptNewConnection(PathfinderVertexID newConnection){		
		acceptedConnections.add(newConnection);
	}
	
	/**
	 * @return
	 */
	public boolean hasNoIncomingConnections() {
		return acceptedConnections.isEmpty();
	}
	
	
	public SetWritable<PathfinderVertexID> getAcceptedConnections(){
		return acceptedConnections;
	}

	/**
	 * @return the loeDestinationFragment
	 */
	public PathfinderVertexID getLoeDestinationFragment() {
		return loeDestinationFragment;
	}

	/**
	 * @param loeDestinationFragment the loeDestinationFragment to set
	 */
	public void setLoeDestinationFragment(PathfinderVertexID loeDestinationFragment) {
		this.loeDestinationFragment = loeDestinationFragment;
	}
	
	/**
	 * @param id
	 * @return
	 */
	public SetWritable<PathfinderVertexID> getRecipientsForFragment(PathfinderVertexID id) {
		return (SetWritable<PathfinderVertexID>) loeAlternatives.get(id);
	}
	
	public void addToFragmentStack(PathfinderVertexID fragment, PathfinderVertexID recipient){
		if(!loeAlternatives.containsKey(fragment))
			loeAlternatives.put(fragment, new SetWritable<PathfinderVertexID>());
		((SetWritable<PathfinderVertexID>)loeAlternatives.get(fragment)).add(recipient);
	}
	
	public SetWritable<PathfinderVertexID> getSetOutOfStack(PathfinderVertexID setToPop){
//		return (SetWritable<PathfinderVertexID>) loeAlternatives.remove(loeAlternatives.keySet().iterator().next());
		return (SetWritable<PathfinderVertexID>) loeAlternatives.get(setToPop);
	}
	
	@SuppressWarnings("unchecked")
	public SetWritable<PathfinderVertexID> popSetOutOfStack(PathfinderVertexID setToPop){
//		return (SetWritable<PathfinderVertexID>) loeAlternatives.remove(loeAlternatives.keySet().iterator().next());
		return (SetWritable<PathfinderVertexID>) loeAlternatives.remove(setToPop);
	}
	
	public boolean isAcceptedFragment(PathfinderVertexID test){
		return loeAlternatives.containsKey(test);
	}
	
	public boolean isStackEmpty(){
		return loeAlternatives.isEmpty();
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
		this.loesDepleted = true;
	}
	
	public void resetLOEDepleted(){
		this.loesDepleted = false;
	}

	/**
	 * @return the clearedForConnection
	 */
	public boolean isClearedForConnection() {
		return clearedForConnection;
	}

	/**
	 * 
	 */
	public void authorizeConnections() {
		this.clearedForConnection = true;
	}
	
	/**
	 * 
	 */
	public void deAuthorizeConnections() {
		this.clearedForConnection = false;
	}

	/**
	 * @return the boruvkaStatus
	 */
	public boolean boruvkaStatus() {
		return boruvkaStatus;
	}

	public void reactivateForBoruvka(){
		this.boruvkaStatus = true;
	}

	/**
	 * @param boruvkaStatus the boruvkaStatus to set
	 */
	public void deactivateForBoruvka() {
		this.boruvkaStatus = false;
	}

	@SuppressWarnings("unchecked")
	public void retainLOE(PathfinderVertexID loeToRetain){
		MapWritable temp = new MapWritable();
		temp.put(loeToRetain, new SetWritable<PathfinderVertexID>((SetWritable<PathfinderVertexID>) loeAlternatives.get(loeToRetain)));
		loeAlternatives = temp;
	}

	/**
	 * @return the pingedByRoot
	 */
	public boolean isPingedByRoot() {
		return pingedByRoot;
	}

	/**
	 * @param pingedByRoot the pingedByRoot to set
	 */
	public void setPingedByRoot(boolean pingedByRoot) {
		this.pingedByRoot = pingedByRoot;
	}

	/**
	 * @return the branchConnection
	 */
	public boolean isBranchConnectionEnabled() {
		return branchConnection;
	}

	/**
	 * 
	 */
	public void authorizeBranchConnection() {
		this.branchConnection = true;
	}
	
	/**
	 * 
	 */
	public void deauthorizeBranchConnection() {
		this.branchConnection = false;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		isRoot = in.readBoolean();
		boruvkaStatus = in.readBoolean();
		loesDepleted = in.readBoolean();
		loeValue = in.readDouble();
		fragmentIdentity.readFields(in);
		loeDestinationFragment.readFields(in);
		clearedForConnection = in.readBoolean();
		loeAlternatives.readFields(in);
		pingedByRoot = in.readBoolean();
		branchConnection = in.readBoolean();
		acceptedConnections.readFields(in);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeBoolean(isRoot);
		out.writeBoolean(boruvkaStatus);
		out.writeBoolean(loesDepleted);
		out.writeDouble(loeValue);
		fragmentIdentity.write(out);
		loeDestinationFragment.write(out);
		loeAlternatives.write(out);
		out.writeBoolean(pingedByRoot);
		out.writeBoolean(branchConnection);
		acceptedConnections.write(out);
	}

}
