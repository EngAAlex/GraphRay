/**
 * 
 */
package com.graphray.common.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Stack;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/**
 * @author spark
 *
 */
public class SetWritable<T extends Writable> implements Writable, Collection<T>{

	Stack<T> internalState;

	public SetWritable(){
		internalState = new Stack<T>();		
	}

	public SetWritable(SetWritable<T> toCopy){
		internalState = new Stack<T>();		
		internalState.addAll(toCopy);
	}

	public boolean add(T e){
		return internalState.add(e);
	}

	/* (non-Javadoc)
	 * @see java.util.Collection#addAll(java.util.Collection)
	 */
	public boolean addAll(Collection<? extends T> c) {
		return internalState.addAll(c);

	}


	/**
	 * @param recipientsForFragment
	 */
	public void addIfNotExistingAll(SetWritable<T> recipientsForFragment) {
		for(T current : recipientsForFragment)
			if(internalState.contains(current))
				internalState.push(current);
	}

	public T pop(){
		return internalState.pop();
	}

	/**
	 * @return
	 */
	public T peek() {
		return internalState.peek();
	}

	public void clear(){
		internalState.clear();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		int size = internalState.size();
		out.writeInt(size);
		if(size > 0)
			for(T p : internalState)
				p.write(out);
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for(int i=0; i < size; i++){
			//WARNING !!!!!! VULNERABILITY, POTENTIAL PROBLEMS WHILE DESERIALIZING
			
			T element = (T) WritableFactories.newInstance((Class<? extends Writable>)(getClass().getTypeParameters()[0].getClass()));
			element.readFields(in);
			internalState.add(i, element);
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<T> iterator() {
		return internalState.iterator();
	}

	/**
	 * @return
	 */
	public boolean isEmpty() {
		return internalState.isEmpty();
	}


	/* (non-Javadoc)
	 * @see java.util.Collection#contains(java.lang.Object)
	 */
	public boolean contains(Object o) {
		return internalState.contains(o);
	}

	/* (non-Javadoc)
	 * @see java.util.Collection#containsAll(java.util.Collection)
	 */
	public boolean containsAll(Collection<?> c) {
		return internalState.containsAll(c);
	}

	/* (non-Javadoc)
	 * @see java.util.Collection#remove(java.lang.Object)
	 */
	public boolean remove(Object o) {
		return internalState.remove(o);
	}

	/* (non-Javadoc)
	 * @see java.util.Collection#removeAll(java.util.Collection)
	 */
	public boolean removeAll(Collection<?> c) {
		return internalState.removeAll(c);
	}

	/* (non-Javadoc)
	 * @see java.util.Collection#retainAll(java.util.Collection)
	 */
	public boolean retainAll(Collection<?> c) {
		return internalState.retainAll(c);
	}

	/* (non-Javadoc)
	 * @see java.util.Collection#size()
	 */
	public int size() {
		return internalState.size();
	}

	/* (non-Javadoc)
	 * @see java.util.Collection#toArray()
	 */
	public Object[] toArray() {
		return internalState.toArray();
	}

	/* (non-Javadoc)
	 * @see java.util.Collection#toArray(java.lang.Object[])
	 */
	public <T> T[] toArray(T[] a) {
		return internalState.toArray(a);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String toReturn = "";
		if(internalState.isEmpty()){
			return "Empty set";
		}else
			for(T current : internalState)
				toReturn += current.toString() + ",";
		return toReturn;
	}

}
