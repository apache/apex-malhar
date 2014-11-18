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
package com.datatorrent.demos.goldengate;

import com.datatorrent.lib.db.jdbc.AbstractJdbcNonTransactionableOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcStore;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * This operator queries an oracle database to get the most recent entries in the employee table.
 */
public class OracleDBOutputOperator extends AbstractJdbcNonTransactionableOutputOperator<Employee, JdbcStore>
{
  private static final String INSERT = "INSERT INTO PROCESSEDEMPLOYEE" +
                                        " (EID, NAME, DEPARTMENT)" +
                                        " values (?, ?, ?)";
  public OracleDBOutputOperator()
  {
    store = new JdbcStore();
  }

  public JdbcStore getStore()
  {
    return store;
  }

  public void setStore(JdbcStore store)
  {
    this.store = store;
  }

  @Override
  protected String getUpdateCommand()
  {
    return INSERT;
  }

  @Override
  protected void setStatementParameters(PreparedStatement statement,
                                        Employee tuple) throws SQLException
  {
    statement.setInt(1, tuple.eid);
    statement.setString(2, tuple.ename);
    statement.setInt(3, tuple.did);
  }
}
