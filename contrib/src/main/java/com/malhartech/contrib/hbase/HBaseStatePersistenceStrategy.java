/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTable;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public interface HBaseStatePersistenceStrategy
{
  public void setTable(HTable table);
  public HTable getTable();
  public void setup() throws IOException;
  public byte[] getState(byte[] name) throws IOException;
  public void saveState(byte[] name, byte[]    value) throws IOException;
}
