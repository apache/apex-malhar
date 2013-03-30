/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Interface for persistence strategy.<br>
 *
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
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
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
