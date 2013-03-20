/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
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
  public static final byte[] colfam0_bytes = Bytes.toBytes("colfam0");
  public static final byte[] col0_bytes = Bytes.toBytes("col-0");

  public static void populateHBase() throws IOException {
      Configuration conf = getConfiguration();
      clearHBase(conf);
      HTable table1 = new HTable(conf, "table1");
      for (int i=0; i < 500; ++i) {
        Put put = new Put(Bytes.toBytes("row" + i));
        for (int j=0; j < 500; ++j) {
          put.add(colfam0_bytes, Bytes.toBytes("col" + "-" +  j ), Bytes.toBytes("val" + "-" + i + "-" + j));
        }
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
    HColumnDescriptor cdesc = new HColumnDescriptor(colfam0_bytes);
    tdesc.addFamily(cdesc);
    admin.createTable(tdesc);
  }

  public static HBaseTuple getHBaseTuple(String row, String colFamily, String colName) throws IOException {
    Configuration conf = getConfiguration();
    HTable table1 = new HTable(conf, "table1");
    Get get = new Get(Bytes.toBytes(row));
    get.addFamily(Bytes.toBytes(colFamily));
    get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName));
    Result result = table1.get(get);
    return getHBaseTuple(result);
  }

  public static HBaseTuple getHBaseTuple(Result result) {
    HBaseTuple tuple = null;
    KeyValue[] kvs = result.raw();
    for (KeyValue kv : kvs) {
      tuple = getHBaseTuple(kv);
      break;
    }
    return tuple;
  }

  public static HBaseTuple getHBaseTuple(KeyValue kv) {
      HBaseTuple tuple = new HBaseTuple();
      tuple.setRow(new String(kv.getRow()));
      tuple.setColFamily(new String(kv.getFamily()));
      tuple.setColName(new String(kv.getQualifier()));
      tuple.setColValue(new String(kv.getValue()));
      return tuple;
  }

  public static HBaseTuple findTuple(List<HBaseTuple> tuples, String row, String colFamily, String colName) {
    HBaseTuple mtuple = null;
    for (HBaseTuple tuple : tuples) {
      if (tuple.getRow().equals(row) && tuple.getColFamily().equals(colFamily)
              && tuple.getColName().equals(colName)) {
        mtuple = tuple;
        break;
      }
    }
    return mtuple;
  }

  private static Configuration getConfiguration() {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "127.0.0.1");
    conf.set("hbase.zookeeper.property.clientPort", "2822");
    return conf;
  }

}
