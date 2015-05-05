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
package com.datatorrent.contrib.memsql;

import com.datatorrent.api.Context.OperatorContext;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

/*
 * A generic implementation of AbstractMemsqlInputOperator which can emit any POJO.
 */
public class MemsqlInputOperator extends AbstractMemsqlInputOperator<Object>
{
  private int currentRow = 1;
  private int inputSize = 0;
  private String tablename;
  private String primaryKeyColumn;
  private Long currentPrimaryKey;
  private int batchsize;

  /*
   * Records are read in batches of this size.
   * Gets the batch size.
   * @return batchsize
   */
  public int getBatchsize()
  {
    return batchsize;
  }

  /*
   * Records are read in batches of this size.
   * Sets the batch size.
   * @param: batchsize;
   */
  public void setBatchsize(int batchsize)
  {
    this.batchsize = batchsize;
  }

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

  /*
   * Name of the table in Memsql Database.
   * Sets the Memsql Tablename.
   * @param tablename
   */
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

    String minPrimaryKeyStmt = "select min " + primaryKeyColumn + " FROM " + tablename;
    try {
      Statement statement = store.getConnection().createStatement();
      ResultSet resultSet = statement.executeQuery(minPrimaryKeyStmt);
      if (resultSet.next()) {
        currentPrimaryKey = (Long)resultSet.getObject(1);
      }
      statement.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Object getTuple(ResultSet result)
  {
    Long valueofprimarykeyid = 0L;
    try {
      valueofprimarykeyid = (Long)result.getObject(primaryKeyColumn);
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
    if (valueofprimarykeyid >= currentPrimaryKey) {
      currentPrimaryKey = valueofprimarykeyid + 1;
    }
    Object tuple = new Object();

    try {
      tuple = result.getObject(2);
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    return tuple;
  }

  @Override
  public void emitTuples()
  {
    super.emitTuples();
  }

  @Override
  public String queryToRetrieveData()
  {
    if (currentRow > inputSize) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("select * from ");
    sb.append(tablename);
    sb.append(" where ");
    sb.append(primaryKeyColumn);
    sb.append(" >= ");
    sb.append(currentRow);
    sb.append(" and ");
    sb.append("LIMIT");
    sb.append(batchsize);

    return sb.toString();
  }

}
