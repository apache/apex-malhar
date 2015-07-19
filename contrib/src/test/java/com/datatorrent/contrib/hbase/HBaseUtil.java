package com.datatorrent.contrib.hbase;

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
    try
    {
      admin = new HBaseAdmin( configuration );
      
      if (!admin.isTableAvailable(tableName) )
      {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("f0"));
        tableDescriptor.addFamily(new HColumnDescriptor("f1"));

        admin.createTable(tableDescriptor);
      }

    }
    finally
    {
      if (admin != null)
      {
        admin.close();
      }
    }
  }
  
  public static void deleteTable( Configuration configuration, String tableName ) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
  {
    HBaseAdmin admin = null;
    try
    {
      admin = new HBaseAdmin( configuration );
      
      if ( admin.isTableAvailable(tableName) )
      {
        admin.disableTable(tableName);
        admin.deleteTable( tableName );
      }
    }
    finally
    {
      if (admin != null)
      {
        admin.close();
      }
    }
  }
}
