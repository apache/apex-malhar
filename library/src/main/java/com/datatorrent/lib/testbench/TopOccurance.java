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
package com.datatorrent.lib.testbench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This operator consumes key value pairs of strings and integers.&nbsp;
 * If the value of a pair is greater than the specified threshold then the tuple is emitted.
 * <p></p>
 * @displayName Top Occurance
 * @category Testbench
 * @tags numeric, compare
 * @since 0.3.2
 */
public class TopOccurance extends BaseOperator
{
	// n value
	private int n = 5;
	private int threshHold = 5;

  /**
   *
   */
	public final transient DefaultOutputPort<Map<Integer, String>> outport = new DefaultOutputPort<Map<Integer, String>>();
	/**
   *
   */
  public final transient DefaultOutputPort<Map<Integer, String>> gtThreshHold = new DefaultOutputPort<Map<Integer, String>>();

	// input port
	public final transient DefaultInputPort<Map<String, Integer>> inport =
			 new DefaultInputPort<Map<String, Integer>>() {
    @Override
    public void process(Map<String, Integer> tuple)
    {
      int numOuts = 0;
      if (tuple.size() < n)
      {
    	for (Map.Entry<String, Integer> entry : tuple.entrySet())
      	{
      		Map<Integer, String> out = new HashMap<Integer, String>();
      		String value = new StringBuilder(entry.getKey()).append("##").append(entry.getValue()).toString();
      		out.put(numOuts++, value);
      		outport.emit(out);
      	}
      	while(numOuts < n)
      	{
      		Map<Integer, String> out = new HashMap<Integer, String>();
      		out.put(numOuts++, "");
      		outport.emit(out);
      	}
      } else {

		ArrayList<Integer> values = new ArrayList<Integer>();
		for (Map.Entry<String, Integer> entry : tuple.entrySet())
		{
		  values.add(entry.getValue());
		}
		Collections.sort(values);
		for (int i=values.size()-1; i >= 0; i--)
		{
		  for (Map.Entry<String, Integer> entry : tuple.entrySet())
	      {
			if (entry.getValue() == values.get(i))
			{
			  Map<Integer, String> out = new HashMap<Integer, String>();
			  String value = new StringBuilder(entry.getKey()).append("##").append(entry.getValue()).toString();
			  out.put(numOuts++, value);
			  outport.emit(out);
			}
			if (numOuts >= n) break;
	      }
		  if (numOuts >= n) break;
		}
      }

      // output greater than threshhold
      numOuts = 1;
      for (Map.Entry<String, Integer> entry : tuple.entrySet())
      {
      	if (entry.getValue() > threshHold)
      	{
      		Map<Integer, String> out = new HashMap<Integer, String>();
      	    String value = new StringBuilder(entry.getKey()).append("##").append(entry.getValue()).toString();
		    out.put(numOuts++, value);
		    gtThreshHold.emit(out);
		}
      }
      Map<Integer, String> out = new HashMap<Integer, String>();
	  out.put(0,  new Integer(numOuts).toString());
	  gtThreshHold.emit(out);
     }
	};

	public int getN()
	{
		return n;
	}

	public void setN(int n)
	{
		this.n = n;
	}

	public int getThreshHold()
	{
		return threshHold;
	}

	public void setThreshHold(int threshHold)
	{
		this.threshHold = threshHold;
	}

}
