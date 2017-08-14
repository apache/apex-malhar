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
package org.apache.apex.malhar.lib.db.jdbc;

import java.sql.SQLException;

/**
 * A store which is used to connect to a non transactional jdbc database.
 * This class contains helper methods to keep track of which window has
 * been committed.
 *
 * @since 1.0.5
 */
public class JdbcNonTransactionalStore extends JdbcTransactionalStore
{
  @Override
  public final void beginTransaction()
  {
    throw new RuntimeException("Does not support transactions.");
  }

  @Override
  public final void commitTransaction()
  {
    throw new RuntimeException("Does not support transactions.");
  }

  @Override
  public final void rollbackTransaction()
  {
    throw new RuntimeException("Does not support transactions.");
  }

  @Override
  public final boolean isInTransaction()
  {
    throw new RuntimeException("Does not support transactions.");
  }

  @Override
  public void connect()
  {
    super.connect();

    try {
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    Long lastWindowCommit = getCommittedWindowIdHelper(appId, operatorId);

    if (lastWindowCommit == null) {
      return -1L;
    } else {
      return lastWindowCommit;
    }
  }

  @Override
  public void disconnect()
  {
    try {
      super.disconnect();

      lastWindowFetchCommand.close();
      lastWindowInsertCommand.close();
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }
}
