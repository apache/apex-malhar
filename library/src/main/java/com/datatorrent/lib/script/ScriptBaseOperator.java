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
package com.datatorrent.lib.script;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Abstract script operator, implementing common functions.
 *
 * @since 0.3.2
 */
public abstract class ScriptBaseOperator extends BaseOperator implements ScriptOperator
{
	/**
	 * Pass thru flag, enables output.
	 */
	protected boolean isPassThru;
	
	/**
	 * Script code for execution.
	 */
	protected String  scriptCode;
	
	/**
	 * Variable value map.
	 */
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
