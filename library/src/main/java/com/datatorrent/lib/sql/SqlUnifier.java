package com.datatorrent.lib.sql;

import java.util.HashMap;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

public class SqlUnifier implements Unifier<HashMap<String, Object>>
{

	/**
	 * Unifier output port
	 */
	public final transient DefaultOutputPort<HashMap<String, Object>> outport = new DefaultOutputPort<HashMap<String, Object>>();
	 
	@Override
	public void beginWindow(long arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void endWindow()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setup(OperatorContext arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void teardown()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void process(HashMap<String, Object> tuple)
	{
		outport.emit(tuple);
	}

}
