/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import java.io.IOException;
import java.math.BigDecimal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseTestHelper
{

  public static final byte[] table_bytes = Bytes.toBytes("table1");
  public static final byte[] cf1_bytes = Bytes.toBytes("cf1");
  public static final byte[] col1_bytes = Bytes.toBytes("col1");
  public static final byte[] col2_bytes = Bytes.toBytes("col2");

  public static void populateHBase() throws IOException {
      Configuration conf = getConfiguration();
      clearHBase(conf);
      HTable table1 = new HTable(conf, "table1");
      for ( int i=0; i < 500; ++i ) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.add(cf1_bytes, col1_bytes, Bytes.toBytes("val" + (i) + "-" + 1));
        put.add(cf1_bytes, col2_bytes, Bytes.toBytes("val" + (i) + "-" + 2));
        table1.put(put);
      }
  }

  public static void clearHBase() throws IOException {
    Configuration conf = getConfiguration();
    clearHBase(conf);
  }

  private static void clearHBase(Configuration conf) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    if (admin.isTableAvailable(table_bytes)) {
        admin.disableTable(table_bytes);
        admin.deleteTable(table_bytes);
    }
    HTableDescriptor tdesc = new HTableDescriptor(table_bytes);
    HColumnDescriptor cdesc = new HColumnDescriptor(cf1_bytes);
    tdesc.addFamily(cdesc);
    admin.createTable(tdesc);
  }

  public static HBaseTuple getHBaseTuple(String row, String colFamily) throws IOException {
    Configuration conf = getConfiguration();
    HTable table1 = new HTable(conf, "table1");
    Get get = new Get(Bytes.toBytes(row));
    get.addFamily(Bytes.toBytes(colFamily));
    Result result = table1.get(get);
    return getHBaseTuple(result);
  }

  public static HBaseTuple getHBaseTuple(Result result) {
      HBaseTuple tuple = new HBaseTuple();
      tuple.setRow(new String(result.getRow()));
      KeyValue[] kvs = result.raw();
      for (KeyValue kv : kvs) {
        if (kv.matchingQualifier(col1_bytes)) {
          tuple.setColFamily(new String(kv.getFamily()));
          tuple.setCol1Value(new String(kv.getValue()));
        }
        else if (kv.matchingQualifier(col2_bytes)) {
          tuple.setCol2Value(new String(kv.getValue()));
        }
      }
      return tuple;
  }

  private static Configuration getConfiguration() {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "127.0.0.1");
    conf.set("hbase.zookeeper.property.clientPort", "2822");
    return conf;
  }

}
