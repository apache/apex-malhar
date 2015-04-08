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
import com.google.common.base.Preconditions;

import java.util.List;

@DataType(type=GenericDataQueryDimensional.TYPE)
@DataSerializerInfo(clazz=GenericDataResultDimensionalSerializer.class)
public class GenericDataResultDimensional extends GenericDataResultTabular
{
  public static final String TYPE = "dataResult";

  private List<GPOMutable> keys;

  public GenericDataResultDimensional(GenericDataQueryDimensional dataQuery,
                           List<GPOMutable> keys,
                           List<GPOMutable> values,
                           long countdown)
  {
    super((GenericDataQueryTabular) dataQuery,
          values,
          countdown);
    setKeys(keys);

    if(keys.size() != values.size()) {
      throw new IllegalArgumentException("The keys " + keys.size() +
                                         " and values " + values.size() +
                                         " arrays must be the same size.");
    }
  }


  @Override
  public GenericDataQueryDimensional getQuery()
  {
    return (GenericDataQueryDimensional) super.getQuery();
  }

  private void setKeys(List<GPOMutable> keys)
  {
    Preconditions.checkNotNull(keys);
    this.keys = keys;
  }

  public List<GPOMutable> getKeys()
  {
    return keys;
  }
}
