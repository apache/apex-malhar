/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.DataSerializerInfo;
import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.Result;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

@DataType(type=DataQueryDimensional.TYPE)
@DataSerializerInfo(clazz=DataResultDimensionalSerializer.class)
public class DataResultDimensional extends Result
{
  public static final String TYPE = "dataResult";

  private List<Map<String, GPOMutable>> keys;
  private List<Map<String, GPOMutable>> values;

  public DataResultDimensional(DataQueryDimensional dataQuery,
                               List<Map<String, GPOMutable>> keys,
                               List<Map<String, GPOMutable>> values)
  {
    super(dataQuery);
    setKeys(keys);
    setValues(values);

    initialize();
  }

  public DataResultDimensional(DataQueryDimensional dataQuery,
                           List<Map<String, GPOMutable>> keys,
                           List<Map<String, GPOMutable>> values,
                           long countdown)
  {
    super(dataQuery,
          countdown);
    setKeys(keys);
    setValues(values);

    initialize();
  }

  private void initialize()
  {
    if(keys.size() != values.size()) {
      throw new IllegalArgumentException("The keys " + keys.size()
                                         + " and values " + values.size()
                                         + " arrays must be the same size.");
    }
  }

  @Override
  public DataQueryDimensional getQuery()
  {
    return (DataQueryDimensional) super.getQuery();
  }

  private void setKeys(List<Map<String, GPOMutable>> keys)
  {
    this.keys = Preconditions.checkNotNull(keys, "keys");
  }

  public List<Map<String, GPOMutable>> getKeys()
  {
    return keys;
  }

  private void setValues(List<Map<String, GPOMutable>> values)
  {
    this.values = Preconditions.checkNotNull(values, "values");
  }

  public List<Map<String, GPOMutable>> getValues()
  {
    return values;
  }
}
