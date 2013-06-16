/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.datatorrent.lib.testbench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class TopOccurance extends BaseOperator
{
	// n value  
	private int n = 5;
	private int threshHold = 5;
	
  //out port
	public final transient DefaultOutputPort<Map<Integer, String>> outport = new DefaultOutputPort<Map<Integer, String>>(this);
	public final transient DefaultOutputPort<Map<Integer, String>> gtThreshHold = new DefaultOutputPort<Map<Integer, String>>(this);
  
	// input port    
	public final transient DefaultInputPort<Map<String, Integer>> inport = 
			 new DefaultInputPort<Map<String, Integer>>(this) {
    @Override
    public void process(Map<String, Integer> tuple) {
    	
    	int numOuts = 0;
      if (tuple.size() < n)
      {   
      	for (Map.Entry<String, Integer> entry : tuple.entrySet())
      	{
      		Map<Integer, String> out = new HashMap<Integer, String>(); 
      		out.put(numOuts++, entry.getKey());
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
							out.put(numOuts++, entry.getKey());
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
					out.put(numOuts++, entry.getKey());
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
