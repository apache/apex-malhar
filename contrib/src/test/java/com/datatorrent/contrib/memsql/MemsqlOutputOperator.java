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
import static com.datatorrent.contrib.memsql.AbstractMemsqlOutputOperatorTest.FQ_TABLE;
import java.sql.*;

public class MemsqlOutputOperator extends AbstractMemsqlOutputOperator<Integer>
{
  private static final String INSERT = "INSERT INTO " +
                                        FQ_TABLE +
                                        " (" + AbstractMemsqlOutputOperatorTest.DATA_COLUMN + ")" +
                                        " values (?)";

  public MemsqlOutputOperator()
  {
  }

  @Override
  protected String getUpdateCommand()
  {
    return INSERT;
  }

  @Override
  protected void setStatementParameters(PreparedStatement statement, Integer tuple) throws SQLException
  {
    statement.setInt(1, tuple);
  }
}
