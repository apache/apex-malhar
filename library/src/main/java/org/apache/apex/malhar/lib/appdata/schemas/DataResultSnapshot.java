/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.appdata.schemas;

import java.util.List;

import org.apache.apex.malhar.lib.appdata.gpo.GPOMutable;
import org.apache.apex.malhar.lib.appdata.query.serde.DataResultSnapshotSerializer;
import org.apache.apex.malhar.lib.appdata.query.serde.MessageSerializerInfo;
import org.apache.apex.malhar.lib.appdata.query.serde.MessageType;

import com.google.common.base.Preconditions;

/**
 * This class represents the result sent in response to a {@link DataQuerySnapshot}.
 * @since 3.0.0
 */
@MessageType(type = DataResultSnapshot.TYPE)
@MessageSerializerInfo(clazz = DataResultSnapshotSerializer.class)
public class DataResultSnapshot extends Result
{
  /**
   * The AppData type of the result.
   */
  public static final String TYPE = "dataResult";

  private List<GPOMutable> values;

  /**
   * This creates a {@link DataResultSnapShot} object from the given query and
   * list of values.
   * @param query The query that this result is a response to.
   * @param values The result values for the query.
   */
  public DataResultSnapshot(Query query, List<GPOMutable> values)
  {
    super(query);

    setValues(values);
  }

  /**
   * This creates a {@link DataResultSnapShot} object from the given query,
   * list of values, and countdown value.
   * @param query The query that this result is a response to.
   * @param values The result values for the query.
   * @param countdown The countdown value for the result.
   */
  public DataResultSnapshot(Query query, List<GPOMutable> values, long countdown)
  {
    super(query, countdown);

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
   * Gets the values for this result.
   * @return The values for this result.
   */
  public List<GPOMutable> getValues()
  {
    return values;
  }
}
