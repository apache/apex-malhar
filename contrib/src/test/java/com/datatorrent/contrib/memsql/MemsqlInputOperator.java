/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.contrib.memsql;

import com.datatorrent.api.Context.OperatorContext;
import static com.datatorrent.contrib.memsql.AbstractMemsqlOutputOperatorTest.*;
import java.sql.*;

public class MemsqlInputOperator extends AbstractMemsqlInputOperator<Integer, MemsqlStore>
{
  private static final String SELECT_COUNT = "select count(*) from " + FQ_TABLE;
  private int blastSize = 0;
  private int currentRow = 1;
  private int inputSize = 0;

  public MemsqlInputOperator()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    try {
      Statement statement = store.getConnection().createStatement();
      ResultSet resultSet = statement.executeQuery(SELECT_COUNT);
      resultSet.next();
      inputSize = resultSet.getInt(1);
      statement.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Integer getTuple(ResultSet result)
  {
    Integer tuple = null;

    try {
      tuple = result.getInt(2);
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    return tuple;
  }

  @Override
  public void emitTuples()
  {
    if (currentRow >= inputSize) {
      return;
    }

    super.emitTuples();
  }

  @Override
  public String queryToRetrieveData()
  {
    if (currentRow > inputSize) {
      return null;
    }

    int endRow = currentRow + blastSize;

    if (endRow > inputSize + 1) {
      endRow = inputSize + 1;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("select * from ");
    sb.append(FQ_TABLE);
    sb.append(" where ");
    sb.append(INDEX_COLUMN);
    sb.append(" >= ");
    sb.append(currentRow);
    sb.append(" and ");
    sb.append(INDEX_COLUMN);
    sb.append(" < ");
    sb.append(endRow);

    currentRow = endRow;

    return sb.toString();
  }

  public void setBlastSize(int blastSize)
  {
    this.blastSize = blastSize;
  }
}
