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
package org.apache.apex.malhar.contrib.memsql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.validation.constraints.NotNull;
import com.datatorrent.api.Context.OperatorContext;

public class MemsqlInputOperator extends AbstractMemsqlInputOperator<Object>
{
  private int blastSize = 1000;
  private int currentRow = 1;
  private int inputSize = 0;
  @NotNull
  private String tablename;
  @NotNull
  private String primaryKeyColumn;

  /*
   * Primary Key Column of table.
   * Gets the primary key column of memsql table.
   */
  public String getPrimaryKeyColumn()
  {
    return primaryKeyColumn;
  }

  /*
   * Primary Key Column of table.
   * Sets the primary key column of memsql table.
   */
  public void setPrimaryKeyCol(String primaryKeyColumn)
  {
    this.primaryKeyColumn = primaryKeyColumn;
  }

  /*
   * Name of the table in Memsql Database.
   * Gets the Memsql Tablename.
   * @return tablename
   */
  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  public MemsqlInputOperator()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    try {
      Statement statement = store.getConnection().createStatement();
      ResultSet resultSet = statement.executeQuery("select count(*) from " + tablename);
      resultSet.next();
      inputSize = resultSet.getInt(1);
      statement.close();
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Integer getTuple(ResultSet result)
  {
    Integer tuple = null;

    try {
      tuple = result.getInt(2);
    } catch (SQLException ex) {
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
    sb.append(tablename);
    sb.append(" where ");
    sb.append(primaryKeyColumn);
    sb.append(" >= ");
    sb.append(currentRow);
    sb.append(" and ");
    sb.append(primaryKeyColumn);
    sb.append(" < ");
    sb.append(endRow);

    currentRow = endRow;

    return sb.toString();
  }

  public void setBlastSize(int blastSize)
  {
    this.blastSize = blastSize;
  }

  /*
   * Records are read in batches of this size.
   * Gets the batch size.
   * @return batchsize
   */
  public int getBlastSize()
  {
    return blastSize;
  }
}
