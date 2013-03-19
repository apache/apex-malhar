/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.stram.StramLocalCluster;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseGetOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(HBaseGetOperatorTest.class);
  private static final byte[] table_bytes = Bytes.toBytes("table1");
  private static final byte[] cf1_bytes = Bytes.toBytes("cf1");
  private static final byte[] col1_bytes = Bytes.toBytes("col1");
  private static final byte[] col2_bytes = Bytes.toBytes("col2");

  private static List<HBaseTuple> tuples;

  public HBaseGetOperatorTest()
  {
  }

  @Test
  public void testGet()
  {
    System.out.println("run");
    try {
      populateHBase();
      DAG dag = new DAG();
      TestHBaseGetOperator thop = dag.addOperator("testhbaseget", TestHBaseGetOperator.class);
      TupleCollector tc = dag.addOperator("tuplecollector", TupleCollector.class);
      dag.addStream("ss", thop.outputPort, tc.input);

      thop.setTableName("table1");
      thop.setZookeeperQuorum("127.0.0.1");
      thop.setZookeeperClientPort(2822);

      StramLocalCluster lc = new StramLocalCluster(dag);
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(10000);
      /*
      tuples = new ArrayList<HBaseTuple>();
      TestHBaseGetOperator thop = new TestHBaseGetOperator();
           thop.setTableName("table1");
      thop.setZookeeperQuorum("127.0.0.1");
      thop.setZookeeperClientPort(2822);
      thop.setupConfiguration();

      thop.emitTuples();
      */

      // TODO review the generated test code and remove the default call to fail.
      //fail("The test case is a prototype.");
      // Check total number
      System.out.println("tuples size " + tuples.size());
      assert tuples.size() > 0;
      assert tuples.get(0).getCol1val().equals("val01");
      assert tuples.get(0).getCol2val().equals("val02");
      assert tuples.size() >= 499;
      assert tuples.get(499).getCol1val().equals("val4991");
      assert tuples.get(499).getCol2val().equals("val4992");
    } catch (Exception ex) {
      logger.error(ex.getMessage());
      assert false;
    }
  }

  private void populateHBase() throws IOException {
      Configuration conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.quorum", "127.0.0.1");
      conf.set("hbase.zookeeper.property.clientPort", "2822");
      HBaseAdmin admin = new HBaseAdmin(conf);
      if (admin.isTableAvailable(table_bytes)) {
        admin.disableTable(table_bytes);
        admin.deleteTable(table_bytes);
      }
      HTableDescriptor tdesc = new HTableDescriptor(table_bytes);
      HColumnDescriptor cdesc = new HColumnDescriptor(cf1_bytes);
      tdesc.addFamily(cdesc);
      admin.createTable(tdesc);
      HTable table1 = new HTable(conf, "table1");
      for ( int i=0; i < 500; ++i ) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.add(cf1_bytes, col1_bytes, Bytes.toBytes("val" + (i) + 1));
        put.add(cf1_bytes, col2_bytes, Bytes.toBytes("val" + (i) + 2));
        table1.put(put);
      }
  }

  public static class TestHBaseGetOperator extends HBaseGetOperator<HBaseTuple>
  {

    private int rowIndex = 0;

    public Get operationGet()
    {
      Get get = new Get(Bytes.toBytes("row" + rowIndex++));
      get.addFamily(cf1_bytes);
      if (rowIndex >= 500) rowIndex = 0;
      return get;
    }

    @Override
    protected HBaseTuple getTuple(KeyValue[] kvs)
    {
      HBaseTuple testTuple = new HBaseTuple();
      for (KeyValue kv : kvs) {
        if (kv.matchingQualifier(col1_bytes)) {
          testTuple.setCol1val(new String(kv.getValue()));
        }
        else if (kv.matchingQualifier(col2_bytes)) {
          testTuple.setCol2val(new String(kv.getValue()));
        }
      }
      return testTuple;
    }

  }

  public static class TupleCollector extends BaseOperator {

        public TupleCollector() {
            tuples = new ArrayList<HBaseTuple>();
        }

        public final transient DefaultInputPort<HBaseTuple> input = new DefaultInputPort<HBaseTuple>(this) {
            public void process(HBaseTuple tuple) {
                tuples.add( tuple );
                System.out.println( new String(tuple.getCol1val()) + " " + new String(tuple.getCol2val()) );
            }
        };

    }

  public static class HBaseTuple implements Serializable
  {
    private String col1val;
    private String col2val;

    public String getCol1val()
    {
      return col1val;
    }

    public void setCol1val(String col1val)
    {
      this.col1val = col1val;
    }

    public String getCol2val()
    {
      return col2val;
    }

    public void setCol2val(String col2val)
    {
      this.col2val = col2val;
    }
  }

}