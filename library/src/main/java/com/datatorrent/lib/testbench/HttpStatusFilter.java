/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.datatorrent.lib.testbench;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator;

/**
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class HttpStatusFilter extends BaseOperator
{
	private String filterStatus;
	private Map<String, Integer> collect;
	public final transient DefaultInputPort<Map<String, String>> inport = new DefaultInputPort<Map<String, String>>(this) {
    @Override
    public void process(Map<String, String> s) {
    	for(Map.Entry<String, String> entry : s.entrySet())
    	{
    		if (!entry.getValue().equals(filterStatus)) continue;
	    	if (collect.containsKey(entry.getKey()))
	    	{
	    		Integer value = (Integer)collect.remove(entry.getKey());
	    		collect.put(entry.getKey(), new Integer(value+1));
	    	} else {
	    		collect.put(entry.getKey(), new Integer(1));
	    	}
    	}
    }
	};

	@Override
	public void setup(OperatorContext context)
	{
		collect  = new HashMap<String, Integer>();
	}

	@Override
	public void teardown()
	{
	}

	@Override
	public void beginWindow(long windowId)
	{
		collect  = new HashMap<String, Integer>();
	}
	
	// out port
	public final transient DefaultOutputPort<Map<Integer, String>> outport = new DefaultOutputPort<Map<Integer, String>>(this);
	
	@Override
	public void endWindow()
	{
		int numOuts = 1;
  	for(Map.Entry<String, Integer> entry : collect.entrySet())
  	{
  		if (entry.getValue() >= 2) continue;
  		Map<Integer, String> out = new HashMap<Integer, String>();
  		out.put(new Integer(numOuts++), entry.getKey());
  		outport.emit(out);
   	}
  	Map<Integer, String> out = new HashMap<Integer, String>();
		out.put(new Integer(0), new Integer(numOuts).toString());
		outport.emit(out);
		collect  = new HashMap<String, Integer>();
	}

	public String getFilterStatus()
	{
		return filterStatus;
	}

	public void setFilterStatus(String filterStatus)
	{
		this.filterStatus = filterStatus;
	}
}
