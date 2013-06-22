package com.datatorrent.lib.script;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;

public class ScriptBaseOperator extends BaseOperator implements ScriptOperator
{
	protected boolean isPassThru;
	protected String  scriptCode;
	protected ArrayList<Map<String, Object>> tuples = new ArrayList<Map<String, Object>>();
	
	@Override
	public void setPassThru(boolean isPassThru)
	{
		this.isPassThru = isPassThru;
	}

	@Override
	public void setScript(String scriptCode)
	{
		this.scriptCode = scriptCode;
	}

	@Override
	public void setScriptPath(String path)
	{
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line;
			StringBuilder builder = new StringBuilder();
			while ((line = br.readLine()) != null) 
			{
				builder.append(line).append("\n");
			}
			scriptCode = builder.toString();
		} catch (Exception e) {
		}
	}
	
	public void setTuple(Map<String, Object> tuple) 
	{
		tuples.add(tuple);
	}
	
	@Override
  public void beginWindow(long windowId)
  {
	  tuples = new ArrayList<Map<String, Object>>();
  }    
	     
	public final transient DefaultOutputPort<Map<String, Object>> result = new DefaultOutputPort<Map<String, Object>>();
	@Override
	public void endWindow()
	{
		if (isPassThru)
		{
			for (Map<String, Object> tuple : tuples)
			{
				result.emit(tuple);
			}
		}
	}
}
