package com.datatorrent.lib.script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import bsh.EvalError;
import bsh.Interpreter;
import com.malhartech.api.DefaultInputPort;

/**
 * Operator to execute bash script on tuples 
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 *
 */
public class BashOperator extends ScriptBaseOperator
{

	public final transient DefaultInputPort<Map<String, Object>> inBindings = 
			new DefaultInputPort<Map<String, Object>>(this)
			{
				@Override
				public void process(Map<String, Object> tuple)
				{
					tuple = executeCode(tuple);
					BashOperator.this.setTuple(tuple);
				}

				private Map<String, Object> executeCode(Map<String, Object> tuple)
				{
					if ((scriptCode == null)||(scriptCode.length() == 0)) return tuple;
					Interpreter interp = new Interpreter(); 
					try
					{
						// execute script in bash interpretor
						StringBuilder builder = new StringBuilder();
						for(Entry<String, Object> entry : tuple.entrySet())
						{
							String key = entry.getKey();
							builder.append(key.replace('`', ' ')).append(" = ");
							if (entry.getValue() instanceof String)
							{
								builder.append("\"");
								builder.append(entry.getValue());
								builder.append("\"");
							} else {
								builder.append(entry.getValue().toString());
							}
							builder.append(";");
						}

						builder.append(scriptCode);
						interp.eval(builder.toString());
					} catch (EvalError e)	{
						e.printStackTrace();
					}
					
				  // return result 
					HashMap<String, Object> result = new HashMap<String, Object>();
					for(Entry<String, Object> entry : tuple.entrySet())
					{
						Object value;
						try
						{
							value = interp.get(entry.getKey());
							result.put(entry.getKey(), value);
						} catch (EvalError e) {
							e.printStackTrace();
						}
					}
					return result;
				}
			};
}
