/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.pi;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

/**
 * <p>An operator which converts a raw value to a named value singleton list.</p>
 * AppDataSnapshotServerMap.input accepts a List<Map<String,Object>> so we use this operator to
 * convert individual values to a singleton list of a named value
 * <p>
 * @displayNamed Value
 * @tags count
 * @since 3.2.0
 */
public class NamedValueList<T> extends BaseOperator
{
  @NotNull
  private String valueName;

	private List<Map<String, T>> valueList;

	public final transient DefaultInputPort<T> inPort = new DefaultInputPort<T>() {
    @Override
    public void process(T val) {
      valueList.get(0).put(valueName, val);
      outPort.emit(valueList);
    }
	};

	public final transient DefaultOutputPort<List<Map<String, T>>> outPort = new DefaultOutputPort<>();

	@Override
	public void setup(OperatorContext context)
	{
		valueList = new ArrayList<Map<String, T>>();
    HashMap<String, T> map = new HashMap<>();
    map.put(valueName, null);
    valueList.add(map);
	}

	@Override
	public void teardown()
	{
	}

	@Override
	public void beginWindow(long windowId)
	{
	}


	@Override
	public void endWindow()
	{
	}

  public String getValueName() {
    return valueName;
  }

  public void setValueName(String name) {
    valueName = name;
  }
}
