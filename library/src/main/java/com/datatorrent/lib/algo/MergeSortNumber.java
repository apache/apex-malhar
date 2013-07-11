/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import java.util.ArrayList;

public class MergeSortNumber<V extends Number> extends MergeSort<V>
{
	/**
	 * Ascending/Desending flag; 
	 */
	private boolean ascending = true;
	
	/**
	 * sort function.
	 */
	@SuppressWarnings("unchecked")
	public  V[] compare(V val1, V val2) {
		V[] result =  (V[]) new Number[2];
		if (ascending) {
  		if (val1.doubleValue() < val2.doubleValue()) {
  			result[0] = val1;
  			result[1] = val2;
  		} else {
  			result[0] = val2;
  			result[1] = val1;
  		}
		} else {
  		if (val1.doubleValue() < val2.doubleValue()) {
  			result[0] = val2;
  			result[1] = val1;
  		} else {
  			result[0] = val1;
  			result[1] = val2;
  		}
		}
		return result;
	}
	
	/**
	 *  Merge class itself is unifier.
	 */
	public Unifier<ArrayList<V>> getUnifierInstance() {
		return new MergeSortNumber<V>();
	}

	public boolean isAscending()
	{
		return ascending;
	}

	public void setAscending(boolean ascending)
	{
		this.ascending = ascending;
	}
}
