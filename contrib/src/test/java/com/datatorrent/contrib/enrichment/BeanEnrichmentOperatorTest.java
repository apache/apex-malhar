package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BeanEnrichmentOperatorTest extends JDBCLoaderTest
{
  public class Order {
    public int OID;
    public int ID;
    public double amount;

    public Order(int oid, int id, double amount) {
      this.OID = oid;
      this.ID = id;
      this.amount = amount;
    }
    public int getOID()
    {
      return OID;
    }

    public void setOID(int OID)
    {
      this.OID = OID;
    }

    public int getID()
    {
      return ID;
    }

    public void setID(int ID)
    {
      this.ID = ID;
    }

    public double getAmount()
    {
      return amount;
    }

    public void setAmount(double amount)
    {
      this.amount = amount;
    }
  }


  @Test
  public void includeSelectedKeys()
  {
    POJOEnrichmentOperator oper = new POJOEnrichmentOperator();
    oper.setStore(testMeta.dbloader);
    oper.setLookupFieldsStr("ID");
    oper.setIncludeFieldsStr("NAME,AGE,ADDRESS");
    oper.outputClass = EmployeeOrder.class;
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);

    oper.beginWindow(1);
    Order tuple = new Order(3, 4, 700);
    oper.input.process(tuple);
    oper.endWindow();

    Assert.assertEquals("includeSelectedKeys: Number of tuples emitted: ", 1, sink.collectedTuples.size());
    Assert.assertEquals("Ouput Tuple: ", "{OID=3, ID=4, amount=700.0, NAME='Mark', AGE=25, ADDRESS='Rich-Mond', SALARY=0.0}", sink.collectedTuples.get(0).toString());
  }
  @Test
  public void includeAllKeys()
  {
    POJOEnrichmentOperator oper = new POJOEnrichmentOperator();
    oper.setStore(testMeta.dbloader);
    oper.setLookupFieldsStr("ID");
    oper.outputClass = EmployeeOrder.class;
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);


    oper.beginWindow(1);
    Order tuple = new Order(3, 4, 700);
    oper.input.process(tuple);
    oper.endWindow();

    Assert.assertEquals("includeSelectedKeys: Number of tuples emitted: ", 1, sink.collectedTuples.size());
    Assert.assertEquals("Ouput Tuple: ", "{OID=3, ID=4, amount=700.0, NAME='Mark', AGE=25, ADDRESS='Rich-Mond', SALARY=65000.0}", sink.collectedTuples.get(0).toString());
  }
}

