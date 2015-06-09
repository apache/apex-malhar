package com.datatorrent.contrib.hbase;

import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.APP_ID;
import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.OPERATOR_ID;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.contrib.common.FieldInfo.SupportType;
import com.datatorrent.contrib.model.Employee;
import com.datatorrent.contrib.model.TupleGenerator;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class HBasePojoPutOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(HBasePojoPutOperatorTest.class);
  private static final int TEST_SIZE = 10000;
  private static final int WINDOW_SIZE = 1500;
  
  private final long startWindowId = Calendar.getInstance().getTimeInMillis();
  public HBasePojoPutOperatorTest()
  {
  }

  /**
   * this test case only test if HBasePojoPutOperator can save data to the
   * HBase. it doesn't test connection to the other operators
   */
  @Test
  public void testPutInternal()
  {
    long windowId = startWindowId;
    try
    {
      HBasePojoPutOperator operator = new HBasePojoPutOperator();
      setupOperator(operator);

      createTable(operator.getStore());

      int countInWindow = 0;
      for (int i = 0; i < TEST_SIZE; ++i)
      {
        if( countInWindow == 0 )
          operator.beginWindow(windowId++);
        operator.processTuple(getNextTuple());
        if( ++countInWindow == WINDOW_SIZE )
        {
          operator.endWindow();
          countInWindow = 0;
        }
      }

    }
    catch (Exception e)
    {
      Log.warn("testPutInternal() exception.", e);
    }
  }

  protected void createTable(HBaseStore store)
  {
    HBaseAdmin admin = null;
    try
    {
      admin = new HBaseAdmin(store.getConfiguration());
      final String tableName = store.getTableName();
      if (!admin.isTableAvailable(tableName))
      {
        HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes.toBytes(store.getTableName()));
        tableDescriptor.addFamily(new HColumnDescriptor("f0"));
        tableDescriptor.addFamily(new HColumnDescriptor("f1"));

        admin.createTable(tableDescriptor);
      }
      else
      {
        // truncate table
      }

    }
    catch (Exception e)
    {
      logger.error("exception", e);
    }
    finally
    {
      if (admin != null)
      {
        try
        {
          admin.close();
        }
        catch (Exception e)
        {
          logger.warn("close admin exception. ", e);
        }
      }
    }
  }

  protected void setupOperator(HBasePojoPutOperator operator)
  {
    configure(operator);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);

    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(
        OPERATOR_ID, attributeMap);
    
    operator.setup(context);
  }

  protected void configure(HBasePojoPutOperator operator)
  {
    HBaseTableInfo tableInfo = new HBaseTableInfo();
    
    tableInfo.setRowOrIdExpression("row");

    List<HBaseFieldInfo> fieldsInfo = new ArrayList<HBaseFieldInfo>();
    {
      HBaseFieldInfo hfi = new HBaseFieldInfo();
      hfi.setColumnName("name");
      hfi.setFamilyName("f0");
      hfi.setColumnExpression("name");
      hfi.setType( SupportType.STRING );
      fieldsInfo.add(hfi);
    }
    {
      HBaseFieldInfo hfi = new HBaseFieldInfo();
      hfi.setColumnName("age");
      hfi.setFamilyName("f0");
      hfi.setColumnExpression("age");
      hfi.setType( SupportType.INTEGER );
      fieldsInfo.add(hfi);
    }
    {
      HBaseFieldInfo hfi = new HBaseFieldInfo();
      hfi.setColumnName("address");
      hfi.setFamilyName("f1");
      hfi.setColumnExpression("address");
      hfi.setType( SupportType.STRING );
      fieldsInfo.add(hfi);
    }

    tableInfo.setFieldsInfo(fieldsInfo);
    operator.setTableInfo(tableInfo);

    // store related information

    // private String zookeeperQuorum;
    // private int zookeeperClientPort;
    // protected String tableName;
    //
    // protected String principal;
    // protected String keytabPath;

    HBaseStore store = new HBaseStore();
    store.setTableName("employee");
    store.setZookeeperQuorum("localhost");
    store.setZookeeperClientPort(2181);
    store.setPrincipal("user1");
    store.setKeytabPath("");

    operator.setStore(store);

  }

  private TupleGenerator<Employee> tupleGenerator;

  protected Object getNextTuple()
  {
    if( tupleGenerator == null )
      tupleGenerator = new TupleGenerator<Employee>( Employee.class );
    
    return tupleGenerator.getNextTuple();
  }



}
