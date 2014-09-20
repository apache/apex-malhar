/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.stream;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.lib.util.BaseKeyOperator;
import java.util.ArrayList;

/**
 * A derivation of BaseKeyOperator that breaks up an ArrayList tuple into Objects. 
 * <p>
 * Takes in an ArrayList and emits each item in the array; mainly used for
 * breaking up an ArrayList tuple into Objects. <br>
 * It is a pass through operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects ArrayList&lt;K&gt;br> <b>item</b>: emits K<br>
 * 
 * @displayName: Array List To Item
 * @category: stream
 * @tag: arraylist, clone
 * @since 0.3.3
 */
@Stateless
public class ArrayListToItem<K> extends BaseKeyOperator<K>
{
	/**
	 * Input array list port.
	 */
	@InputPortFieldAnnotation(name = "data")
	public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>()
	{
		/**
		 * Emit one item at a time
		 */
		@Override
		public void process(ArrayList<K> tuple)
		{
			for (K k : tuple) {
				item.emit(cloneKey(k));
			}
		}
	};

	/**
	 * Output array item port.
	 */
	@OutputPortFieldAnnotation(name = "item")
	public final transient DefaultOutputPort<K> item = new DefaultOutputPort<K>();
}
