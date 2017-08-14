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
package org.apache.apex.malhar.contrib.misc.streamquery.index;

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.streamquery.index.ColumnIndex;

/**
 * <p>An implementation of Column Index that implements filter method based on mid index. </p>
 * <p>
 * @displayName Mid Index
 * @category Stream Manipulators
 * @tags index
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class MidIndex extends ColumnIndex
{
  private int start;
  private int length = 0;

  public MidIndex(@NotNull String column, String alias, int start)
  {
    super(column, alias);
    assert (start >= 0);
    this.start = start;
  }

  @Override
  public void filter(@NotNull  Map<String, Object> row, @NotNull  Map<String, Object> collect)
  {
    if (!row.containsKey(column)) {
      return;
    }
    if (!(row.get(column) instanceof String)) {
      assert (false);
    }
    String name = getColumn();
    if (alias != null) {
      name = alias;
    }

    int endIndex = start + length;
    if ((length == 0) || (endIndex > ((String)row.get(column)).length())) {
      collect.put(name, row.get(column));
    } else {
      collect.put(name, ((String)row.get(column)).substring(start, endIndex));
    }
  }

  public int getLength()
  {
    return length;
  }

  public void setLength(int length)
  {
    assert (length > 0);
    this.length = length;
  }
}

