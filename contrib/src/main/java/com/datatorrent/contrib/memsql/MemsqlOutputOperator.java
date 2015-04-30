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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.validation.constraints.NotNull;

/*
 * A generic implementation of AbstractMemsqlOutputOperator which can take in a POJO.
 */
public class MemsqlOutputOperator extends AbstractMemsqlOutputOperator<Object>
{
  @NotNull
  private String tablename;
  @NotNull
  private ArrayList<String> dataColumns;
  private String insertStatement;

  /*
   * An arraylist of data column names to be set in Memsql database.
   * Gets column names.
   */
  public ArrayList<String> getDataColumns()
  {
    return dataColumns;
  }

  /*
   * An arraylist of data column names to be set in Memsql database.
   * Sets column names.
   */
  public void setDataColumns(ArrayList<String> dataColumns)
  {
    this.dataColumns = dataColumns;
  }


  /*
   * Gets the Memsql Tablename
   */
  public String getTablename()
  {
    return tablename;
  }

  /*
   * Sets the Memsql Tablename
   */
  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    StringBuilder columns = new StringBuilder("");
    StringBuilder values = new StringBuilder("");
    for (int i = 0; i < dataColumns.size(); i++) {
      columns.append(dataColumns.get(i));
      values.append("?");
      if (i < dataColumns.size() - 1) {
        columns.append(",");
        values.append(",");
      }
    }

    insertStatement = "INSERT INTO "
            + tablename
            + " (" + getDataColumns() + ")"
            + " values (" + values + ")";
  }

  public MemsqlOutputOperator()
  {
  }

  @Override
  protected String getUpdateCommand()
  {
    return insertStatement;
  }

  @Override
  protected void setStatementParameters(PreparedStatement statement, Object tuple) throws SQLException
  {
    statement.setObject(1, tuple);
  }

}
