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
package org.apache.apex.malhar.contrib.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseUtil
{
  public static void createTable(Configuration configuration, String tableName ) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
  {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin( configuration );

      if (!admin.isTableAvailable(tableName) ) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("f0"));
        tableDescriptor.addFamily(new HColumnDescriptor("f1"));

        admin.createTable(tableDescriptor);
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  public static void deleteTable( Configuration configuration, String tableName ) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
  {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin( configuration );
      if ( admin.isTableAvailable(tableName) ) {
        admin.disableTable(tableName);
        admin.deleteTable( tableName );
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
}
