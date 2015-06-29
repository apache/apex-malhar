/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hbase;

import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Provides interface for persistence strategy. <br>
 * <p>
 * <br>
 * The interface needs to be implemented by any class that will provide the persistence
 * for the HBase output operator. The output operator will use the persistence strategy
 * to persist processing state into HBase.<br>
 *
 * <br>
 * The table is provided by the operator using the setTable method. It can be used by the
 * implementor to persist the state in an application specific way. There are additional
 * method to retrieve and save state that needs to be implemented by the implementor.<br>
 *
 * <br>
 * @displayName HBase State Persistence Strategy
 * @category Store
 * @tags persistence
 * @since 0.3.2
 */
@Deprecated
public interface HBaseStatePersistenceStrategy
{
  /**
   * Set the table.
   * It is called by the operator to set the table.
   * @param table The table
   */
  public void setTable(HTable table);

  /**
   * Get the table.
   * @return The table
   */
  public HTable getTable();

  /**
   * Setup the persistence.
   * This method is called once by the operator for the persistence
   * strategy to initialize any resources it needs to.
   * @throws IOException
   */
  public void setup() throws IOException;

  /**
   * Retrieve the state.
   * Retrieve the state of a given parameter. The name of the parameter is specified. The
   * implementor should retrieve and return the previously saved value of the parameter if any from
   * the persistence storage.
   * @param name The parameter name
   * @return The previously saved value if there is any, null otherwise
   * @throws IOException
   */
  public byte[] getState(byte[] name) throws IOException;

  /**
   * Save the state.
   * Save the state of a given parameter. The name and value of the parameter are specified.
   * The implementor should save the name and value of the parameter in a persistence storage to
   * be recalled later.
   * @param name The name of the parameter
   * @param value The value of the parameter
   * @throws IOException
   */
  public void saveState(byte[] name, byte[] value) throws IOException;
}
