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
/**
 * 
 */
package com.graphray.common.vertextypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.graphray.common.writables.SetWritable;

public class PathfinderVertexType implements Writable{

	protected boolean isRoot;
	protected boolean boruvkaStatus;
	protected PathfinderVertexID fragmentIdentity;
	protected double loeValue;
	protected int howManyLoes;
	protected double fragmentLoeValue;
//	protected PathfinderVertexID oldFragmentID;
	protected SetWritable<PathfinderVertexID> lastConnectedVertices;
	protected SetWritable<PathfinderVertexID> loeDestinationFragments;	
	protected boolean loesDepleted;
	protected MapWritable loeAlternatives;
	protected SetWritable<PathfinderVertexID> acceptedConnections;
	
//	protected SetWritable<PathfinderVertexID> activeBoundary;

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
		howManyLoes = 0;
		fragmentIdentity = new PathfinderVertexID();
		loeDestinationFragments = new SetWritable<PathfinderVertexID>();		
		loeAlternatives = new MapWritable();
		acceptedConnections = new SetWritable<PathfinderVertexID>();
		lastConnectedVertices = new SetWritable<PathfinderVertexID>();
		howManyLoes = 0;
//		oldFragmentID = new PathfinderVertexID();
//		activeBoundary = new SetWritable<PathfinderVertexID>();
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
	 * @return the howManyLoes
	 */
	public int getHowManyLoes() {
		return howManyLoes;
	}

	/**
	 * @param howManyLoes the howManyLoes to set
	 */
	public void setHowManyLoes(int howManyLoes) {
		this.howManyLoes = howManyLoes;
	}

	public void getReadyForNextRound(){
		resetLOEStack();
		lastConnectedVertices.clear();
		loeDestinationFragments = new SetWritable<PathfinderVertexID>();
//		oldFragmentID = null;
		clearedForConnection = false;
		acceptedConnections.clear(); //= new SetWritable<PathfinderVertexID>();
		setPingedByRoot(false);
		branchConnection = false;
//		fragmentLoeValue = Double.MAX_VALUE;
		howManyLoes = 0;		
		resetLOE();
	}
	/**
	 * 
	 */
	public void resetLOE(){
		updateLOE(Double.MAX_VALUE);
	}
	
	public void setLOEStack(MapWritable loeStack){
		loeAlternatives = loeStack;
	}
	
	public Set<Writable> getActiveFragments(){
		return loeAlternatives.keySet();
	}

	public void resetLOEStack(){
//		loeAlternatives.clear();;
		loeAlternatives = new MapWritable();
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
	

	
//	/**
//	 * @return
//	 */
//	public PathfinderVertexID getOldFragmentID() {
//		return oldFragmentID;
//	}
//	
//	/**
//	 * @param oldFragmentID the oldFragmentID to set
//	 */
//	public void setOldFragmentID(PathfinderVertexID oldFragmentID) {
//		this.oldFragmentID = oldFragmentID;
//	}

	public void clearAcceptedConnections(){
		acceptedConnections.clear();
	}

	public void acceptNewConnection(PathfinderVertexID newConnection){
		if(!acceptedConnections.contains(newConnection))
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
	 * @param loeDestination the loeDestination to set
	 */
	public void addToLastConnectedVertices(PathfinderVertexID loeDestination) {
		if(!lastConnectedVertices.contains(loeDestination))
			lastConnectedVertices.add(loeDestination);
	}


//	public void getLastConnectedFragments(){
//		return this.lastConnectedFragments;
//	}

	/**
	 * @return the loeDestinationFragment
	 */
	public SetWritable<PathfinderVertexID> getLoeDestinationFragments() {
		return loeDestinationFragments;
	}
	
	/**
	 * @return the loeDestinationFragment
	 */
	public PathfinderVertexID popLoeDestinationFragment() {
		return loeDestinationFragments.pop();
	}
	
	/**
	 * @return the loeDestinationFragment
	 */
	public PathfinderVertexID peekLoeDestinationFragment() {
		return loeDestinationFragments.peek();
	}

	/**
	 * @param loeDestinationFragment the loeDestinationFragment to set
	 */
	public void addToLoeDestinationFragment(PathfinderVertexID loeDestinationFragment) {
		if(!loeDestinationFragments.contains(loeDestinationFragment))
			this.loeDestinationFragments.add(loeDestinationFragment);
	}
	
//	/**
//	 * @param id
//	 * @return
//	 */
//	public SetWritable<PathfinderVertexID> getRecipientsForFragment(PathfinderVertexID id) {
//		return (SetWritable<PathfinderVertexID>) loeAlternatives.get(id);
//	}
	
	public void addToFragmentStack(PathfinderVertexID fragment, PathfinderVertexID recipient){
		if(!loeAlternatives.containsKey(fragment))
			loeAlternatives.put(fragment, new SetWritable<PathfinderVertexID>());
		((SetWritable<PathfinderVertexID>)loeAlternatives.get(fragment)).add(recipient);
	}
	
	public SetWritable<PathfinderVertexID> peekSetOutOfStack(PathfinderVertexID setToPop){
		return (SetWritable<PathfinderVertexID>) loeAlternatives.get(setToPop);
	}
	
	@SuppressWarnings("unchecked")
	public SetWritable<PathfinderVertexID> popSetOutOfStack(PathfinderVertexID setToPop){
		return (SetWritable<PathfinderVertexID>) loeAlternatives.remove(setToPop);
	}
//	
//	public boolean isAcceptedFragment(PathfinderVertexID test){
//		return loeAlternatives.containsKey(test);
//	}
	
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
	
//	/**
//	 * @param senderID
//	 * @param fragmentID
//	 */
//	public void addVertexToFragment(PathfinderVertexID senderID, PathfinderVertexID fragmentID) {
//		if(!loeAlternatives.containsKey(fragmentID))
//			loeAlternatives.put(fragmentID.copy(), new SetWritable<PathfinderVertexID>());
//		((SetWritable<PathfinderVertexID>)loeAlternatives.get(fragmentID)).add(senderID.copy());
//	}
//
//	/**
//	 * @param senderID
//	 * @param fragmentI
//	 */
//	public void removeVertexFromFragment(PathfinderVertexID senderID, PathfinderVertexID fragmentID) {
//		if(loeAlternatives.containsKey(fragmentID)){
//			SetWritable<PathfinderVertexID> currentSet = (SetWritable<PathfinderVertexID>) loeAlternatives.get(fragmentID);
//			currentSet.remove(senderID);
//			if(currentSet.size() == 0){
//				loeAlternatives.remove(fragmentID);
//			}
//		}
//	}
	
	/**
	 * @param fragment
	 * @return
	 */
	public boolean loeStackContainsFragment(PathfinderVertexID fragment) {
		return loeAlternatives.containsKey(fragment);
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
		acceptedConnections.clear();
	}
	
//	/**
//	 * @return the activeBoundary
//	 */
//	public SetWritable<PathfinderVertexID> getActiveBoundary() {
//		return activeBoundary;
//	}
//	
//	public void clearBoundary(){
//		activeBoundary.clear();
//	}

//	/**
//	 * @param activeBoundary the activeBoundary to set
//	 */
//	public void addToBoundary(PathfinderVertexID activeBoundaryToAdd) {
//		if(!activeBoundary.contains(activeBoundaryToAdd))
//			activeBoundary.add(activeBoundaryToAdd);
//	}

	public void retainAcceptedConnections(/*Collection<PathfinderVertexID> collectionToRetain*/){
//		acceptedConnections.retainAll(collectionToRetain);
		acceptedConnections.retainAll(lastConnectedVertices);
	}
	
//	public void replaceBoundarySet(Collection<PathfinderVertexID> setToReplace){
//		activeBoundary.clear();
//		for(PathfinderVertexID current : setToReplace)
//			if(!activeBoundary.contains(current))
//				activeBoundary.add(current.copy());
//	}

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
	 * @return the fragmentLoeValue
	 */
	public double getFragmentLoeValue() {
		return fragmentLoeValue;
	}

	/**
	 * @param fragmentLoeValue the fragmentLoeValue to set
	 */
	public void setFragmentLoeValue(double fragmentLoeValue) {
		this.fragmentLoeValue = fragmentLoeValue;
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
		return !acceptedConnections.isEmpty();
		//		return branchConnection;
	}

	/**
	 * 
	 */
	public void authorizeBranchConnection() {
		this.branchConnection = true;
	}
//	
//	/**
//	 * 
//	 */
//	public void deauthorizeBranchConnection() {
//		this.branchConnection = false;
//	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		isRoot = in.readBoolean();
		boruvkaStatus = in.readBoolean();
		loesDepleted = in.readBoolean();
		loeValue = in.readDouble();
		fragmentLoeValue = in.readDouble();
		fragmentIdentity.readFields(in);
		loeDestinationFragments.readFields(in);
		clearedForConnection = in.readBoolean();
		loeAlternatives.readFields(in);
		pingedByRoot = in.readBoolean();
		branchConnection = in.readBoolean();
		acceptedConnections.readFields(in);
//		oldFragmentID.readFields(in);
		lastConnectedVertices.readFields(in);
//		activeBoundary.readFields(in);
		howManyLoes = in.readInt();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isRoot);
		out.writeBoolean(boruvkaStatus);
		out.writeBoolean(loesDepleted);
		out.writeDouble(loeValue);
		out.writeDouble(fragmentLoeValue);
		fragmentIdentity.write(out);
		loeDestinationFragments.write(out);
		out.writeBoolean(clearedForConnection);
		loeAlternatives.write(out);
		out.writeBoolean(pingedByRoot);
		out.writeBoolean(branchConnection);
		acceptedConnections.write(out);
//		oldFragmentID.write(out);
		lastConnectedVertices.write(out);
//		activeBoundary.write(out);
		out.writeInt(howManyLoes);
	}

	/**
	 * @param pfid
	 * @return
	 */
	public boolean isAcceptedConnection(PathfinderVertexID pfid) {
		return acceptedConnections.contains(pfid);
	}

}
