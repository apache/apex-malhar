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
package org.apache.apex.examples.distributeddistinct;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.apache.apex.malhar.lib.algo.UniqueValueCount;
import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 * This operator demonstrates {@link UniqueValueCountAppender} given that the keys and values of the preceding {@link UniqueValueCount} operator
 * are both integers. <br/>
 * It will keep track of the number of all the unique values emitted per key since the application starts.
 *
 * @since 1.0.4
 */
public class IntegerUniqueValueCountAppender extends UniqueValueCountAppender<Integer>
{
  @Override
  public Object processResultSet(ResultSet resultSet)
  {
    Set<Integer> valSet = new HashSet<Integer>();
    try {
      while (resultSet.next()) {
        valSet.add(resultSet.getInt(1));
      }
      return valSet;
    } catch (SQLException e) {
      throw new RuntimeException("while processing the result set", e);
    }
  }

  @Override
  protected void prepareGetStatement(PreparedStatement getStatement, Object key) throws SQLException
  {
    getStatement.setInt(1, (Integer)key);
  }

  @Override
  protected void preparePutStatement(PreparedStatement putStatement, Object key, Object value) throws SQLException
  {
    @SuppressWarnings("unchecked")
    Set<Integer> valueSet = (Set<Integer>)value;
    for (Integer val : valueSet) {
      @SuppressWarnings("unchecked")
      Set<Integer> currentVals = (Set<Integer>)get(key);
      if (!currentVals.contains(val)) {
        batch = true;
        putStatement.setInt(1, (Integer)key);
        putStatement.setInt(2, val);
        putStatement.setLong(3, windowID);
        putStatement.addBatch();
      }
    }
  }

  @Override
  public void endWindow()
  {
    try {
      Statement stmt = store.getConnection().createStatement();
      String keySetQuery = "SELECT DISTINCT col1 FROM " + tableName;
      ResultSet resultSet = stmt.executeQuery(keySetQuery);
      while (resultSet.next()) {
        int val = resultSet.getInt(1);
        @SuppressWarnings("unchecked")
        Set<Integer> valSet = (Set<Integer>)cacheManager.get(val);
        output.emit(new KeyValPair<Object, Object>(val, valSet.size()));
      }
    } catch (SQLException e) {
      throw new RuntimeException("While emitting tuples", e);
    }
  }
}
