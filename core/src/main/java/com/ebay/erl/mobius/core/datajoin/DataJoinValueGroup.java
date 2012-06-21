package com.ebay.erl.mobius.core.datajoin;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.io.WritableComparable;


/**
 * <p>
 * This product is licensed under the Apache License,  Version 2.0, 
 * available at http://www.apache.org/licenses/LICENSE-2.0.
 * 
 * This product contains portions derived from Apache hadoop which is 
 * licensed under the Apache License, Version 2.0, available at 
 * http://hadoop.apache.org.
 * 
 * © 2007 – 2012 eBay Inc., Evan Chiu, Woody Zhou, Neel Sundaresan
 *
 * @param <IV>
 */
public class DataJoinValueGroup<IV extends WritableComparable> implements Iterator<Iterator<IV>>{

	private Iterator<DataJoinValue> values;
	private DataJoinValue nextValue = null;
	private String nextDatasetID = null;
	
	public DataJoinValueGroup(Iterator<DataJoinValue> values){
		this.values = values;
		if(this.values.hasNext()){
			this.nextValue = this.values.next();
			this.nextDatasetID = this.nextValue.getDatasetID().toString();
		}else{
			this.nextValue = null;
			this.nextDatasetID = null;
		}
	}

	public String nextDatasetID(){
		return this.nextDatasetID;
	}
	
	@Override
	public boolean hasNext() {
		if(this.nextValue != null){
			return true;
		}else{
			return false;
		}
	}

	@Override
	public Iterator<IV> next() {
		if(!hasNext()){
			throw new NoSuchElementException();
		}
		return new InternalIterator(nextValue.getDatasetID().toString());
	}

	@Override
	public void remove() {
		
	}
	
	public class InternalIterator implements Iterator<IV>{

		private String datasetID;
		
		public InternalIterator(String datasetID){
			this.datasetID = datasetID;
		}
		
		@Override
		public boolean hasNext() {
			if( DataJoinValueGroup.this.nextValue != null && datasetID.equals(DataJoinValueGroup.this.nextDatasetID)){
				return true;
			}else{
				return false;
			}
		}

		@Override
		public IV next() {
			if(!hasNext()){
				throw new NoSuchElementException();
			}
			IV ret = (IV)DataJoinValueGroup.this.nextValue.getValue();
			if(DataJoinValueGroup.this.values.hasNext()){
				DataJoinValueGroup.this.nextValue = DataJoinValueGroup.this.values.next();
				DataJoinValueGroup.this.nextDatasetID = DataJoinValueGroup.this.nextValue.getDatasetID().toString();
			}else{
				DataJoinValueGroup.this.nextValue = null;
				DataJoinValueGroup.this.nextDatasetID = null;
			}
			return ret;
		}

		@Override
		public void remove() {
			
		}
		
	}
	
}
