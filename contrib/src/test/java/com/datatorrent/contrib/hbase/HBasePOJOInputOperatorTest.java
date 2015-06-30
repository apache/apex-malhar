package com.datatorrent.contrib.hbase;

import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.OPERATOR_ID;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.util.FieldInfo.SupportType;
import com.datatorrent.contrib.util.TableInfo;
import com.datatorrent.contrib.util.TestPOJO;
import com.datatorrent.contrib.util.TupleCacheOutputOperator;
import com.datatorrent.contrib.util.TupleGenerateCacheOperator;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class HBasePOJOInputOperatorTest
{
  public static enum OPERATOR
  {
    GENERATOR,
    HBASEOUTPUT,
    HBASEINPUT,
    OUTPUT
  };
  
  public static class MyGenerator extends TupleGenerateCacheOperator<TestPOJO>
  {
    public MyGenerator()
    {
      this.setTupleType( TestPOJO.class );
    }
  }
  
  private static final Logger logger = LoggerFactory.getLogger( HBasePOJOInputOperatorTest.class );
  private final int TUPLE_NUM = 1000;
  private HBaseStore store;
  private HBasePOJOPutOperator hbaseOutputOperator;
  private HBasePOJOInputOperator hbaseInputOperator;
  
  @Before
  public void prepare() throws Exception
  {
    hbaseInputOperator = new HBasePOJOInputOperator();
    hbaseOutputOperator = new HBasePOJOPutOperator();
    setupOperators();
    HBaseUtil.createTable( store.getConfiguration(), store.getTableName());
  }
  
  @After
  public void cleanup() throws Exception
  {
    HBaseUtil.deleteTable( store.getConfiguration(), store.getTableName());
  }
  

  @Test
  public void test() throws Exception
  {
    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    DAG dag = lma.getDAG();

    // Create ActiveMQStringSinglePortOutputOperator
    MyGenerator generator = dag.addOperator( OPERATOR.GENERATOR.name(), MyGenerator.class);
    generator.setTupleNum( TUPLE_NUM );
    
    hbaseOutputOperator = dag.addOperator( OPERATOR.HBASEOUTPUT.name(), hbaseOutputOperator );

    hbaseInputOperator = dag.addOperator(OPERATOR.HBASEINPUT.name(), hbaseInputOperator);
    
    
    TupleCacheOutputOperator output = dag.addOperator(OPERATOR.OUTPUT.name(), TupleCacheOutputOperator.class);
    
    // Connect ports
    dag.addStream("queue1", generator.outputPort, hbaseOutputOperator.input ).setLocality(DAG.Locality.NODE_LOCAL);
    dag.addStream("queue2", hbaseInputOperator.outputPort, output.inputPort ).setLocality(DAG.Locality.NODE_LOCAL);
    
    
    Configuration conf = new Configuration(false);
    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    //generator.doneLatch.await();
    while(true)
    {
      try
      {
        Thread.sleep(1000);
      }
      catch( Exception e ){}
      
      logger.info( "Received tuple number {}, instance is {}.", output.getReceivedTuples() == null ? 0 : output.getReceivedTuples().size(), System.identityHashCode( output ) );
      if( output.getReceivedTuples() != null && output.getReceivedTuples().size() == TUPLE_NUM )
        break;
    }
    
    lc.shutdown();

    
    validate( generator.getTuples(), output.getReceivedTuples() );
  }

  protected void validate( List<TestPOJO> expected, List<TestPOJO> actual )
  {
    logger.info( "expected size: " + expected.size() );
    logger.info( "actual size: " + actual.size() );
    Assert.assertTrue( String.format( "The expected size {%d} is different from actual size {%d}.", expected.size(), actual.size()  ), expected.size()==actual.size() );
    actual.removeAll(expected);
    Assert.assertTrue( "content not same.", actual.isEmpty() );
  }
  
  protected void setupOperators()
  {
    TableInfo<HBaseFieldInfo> tableInfo = new TableInfo<HBaseFieldInfo>();
    
    tableInfo.setRowOrIdExpression("row");

    List<HBaseFieldInfo> fieldsInfo = new ArrayList<HBaseFieldInfo>();
    fieldsInfo.add( new HBaseFieldInfo( "name", "name", SupportType.STRING, "f0") );
    fieldsInfo.add( new HBaseFieldInfo( "age", "age", SupportType.INTEGER, "f1") );
    fieldsInfo.add( new HBaseFieldInfo( "address", "address", SupportType.STRING, "f1") );

    tableInfo.setFieldsInfo(fieldsInfo);
    
    hbaseInputOperator.setTableInfo(tableInfo);
    hbaseOutputOperator.setTableInfo(tableInfo);
    
    store = new HBaseStore();
    store.setTableName("test");
    store.setZookeeperQuorum("localhost");
    store.setZookeeperClientPort(2181);

    hbaseInputOperator.setStore(store);
    hbaseOutputOperator.setStore(store);
    
    hbaseInputOperator.setPojoTypeName( TestPOJO.class.getName() );
    
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(
        OPERATOR_ID, new AttributeMap.DefaultAttributeMap());
    hbaseInputOperator.setup(context);
    hbaseOutputOperator.setup(context);
  }

}
