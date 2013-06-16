package com.datatorrent.lib.script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

import org.python.core.PyObject;
import org.python.util.PythonInterpreter;
/**
 * Operator to execute python script on tuples 
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 *
 */
public class PythonOperator extends  ScriptBaseOperator
{
	public final transient DefaultInputPort<Map<String, Object>> inBindings = 
			new DefaultInputPort<Map<String, Object>>(this)
			{
				@Override
				public void process(Map<String, Object> tuple)
				{
					tuple = executescriptCode(tuple);
					PythonOperator.this.setTuple(tuple);
				}

				private Map<String, Object> executescriptCode(Map<String, Object> tuple)
				{
					// no code to execute
					if ((PythonOperator.this.scriptCode == null)||(PythonOperator.this.scriptCode.length() == 0))
					{
						return tuple;
					}
					
					// execute python script
					PythonInterpreter interp = new PythonInterpreter();
					interp.exec("import sys");
					for(Entry<String, Object> entry : tuple.entrySet())
					{
						interp.set(entry.getKey(), entry.getValue());
					}
					interp.exec(PythonOperator.this.scriptCode);
					
					// return result 
					HashMap<String, Object> result = new HashMap<String, Object>();
					for(Entry<String, Object> entry : tuple.entrySet())
					{
						PyObject value = interp.get(entry.getKey());
						result.put(entry.getKey(), value.__tojava__(entry.getValue().getClass()));
					}
					return result;
				}
			};
}
