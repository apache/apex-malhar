/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.datatorrent.lib.testbench;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Context.OperatorContext;

/**
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class HttpStatusFilter extends BaseOperator
{
	private String filterStatus;
	private Map<String, Integer> collect;
	public final transient DefaultInputPort<Map<String, String>> inport = new DefaultInputPort<Map<String, String>>() {
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
	public final transient DefaultOutputPort<Map<String, Integer>> outport = new DefaultOutputPort<Map<String, Integer>>();
	
	@Override
	public void endWindow()
	{
		outport.emit(collect);
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
