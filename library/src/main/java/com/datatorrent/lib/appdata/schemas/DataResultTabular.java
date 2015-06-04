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

import com.datatorrent.lib.appdata.query.serde.DataResultTabularSerializer;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.serde.MessageSerializerInfo;
import com.datatorrent.lib.appdata.query.serde.MessageType;
import com.google.common.base.Preconditions;

import java.util.List;

/**
 * This class represents the result sent in response to a {@link DataQueryTablar}.
 */
@MessageType(type=DataQueryDimensional.TYPE)
@MessageSerializerInfo(clazz=DataResultTabularSerializer.class)
public class DataResultTabular extends Result
{
  /**
   * The AppData type of the result.
   */
  public static final String TYPE = "dataResult";

  private List<GPOMutable> values;

  /**
   * This creates a {@link DataResultTabular} object from the given query and
   * list of values.
   * @param query The query that this result is a response to.
   * @param values The result values for the query.
   */
  public DataResultTabular(Query query,
                           List<GPOMutable> values)
  {
    super(query);

    setValues(values);
  }

  /**
   * This creates a {@link DataResultTabular} object from the given query,
   * list of values, and countdown value.
   * @param query The query that this result is a response to.
   * @param values The result values for the query.
   * @param countdown The countdown value for the result.
   */
  public DataResultTabular(Query query,
                           List<GPOMutable> values,
                           long countdown)
  {
    super(query,
          countdown);

    setValues(values);
  }

  /**
   * This is a helper method that sets and validates the result values.
   * @param values The result values.
   */
  private void setValues(List<GPOMutable> values)
  {
    Preconditions.checkNotNull(values);
    this.values = values;
  }

  @Override
  public Query getQuery()
  {
    return super.getQuery();
  }

  /**
   *
   * @return
   */
  public List<GPOMutable> getValues()
  {
    return values;
  }
}
