package com.datatorrent.contrib.memcache;

import net.spy.memcached.AddrUtil;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.contrib.util.TableInfo;
import com.datatorrent.contrib.util.TestPOJO;
import com.datatorrent.contrib.util.TupleGenerator;

@SuppressWarnings("rawtypes")
public class MemcachePOJOOperatorTest
{
  public static final int TUPLE_SIZE = 1000;
  
  private MemcacheStore store;
  
  @Before
  public void setup()
  {
    store = new MemcacheStore();
    store.setServerAddresses(AddrUtil.getAddresses("v1.bright:11211") );
  }
  
  public void cleanup()
  {
    if( store != null )
    {
      try
      {
        store.disconnect();
      }
      catch( Exception e )
      {
        
      }
    }
      
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testMemcacheOutputOperatorInternal() throws Exception
  {
    MemcachePOJOOutputOperator operator = new MemcachePOJOOutputOperator();
    operator.setStore(store);

    TableInfo tableInfo = new TableInfo();
    tableInfo.setRowOrIdExpression( TestPOJO.getRowExpression() );
    tableInfo.setFieldsInfo( TestPOJO.getFieldsInfo() );
    tableInfo.setRowOrIdExpression( TestPOJO.getRowExpression() );
    operator.setTableInfo( tableInfo );

    operator.setup(null);
    
    TupleGenerator<TestPOJO> generator = new TupleGenerator<TestPOJO>( TestPOJO.class );
    
    for( int i=0; i<TUPLE_SIZE; ++i )
    {
      operator.processTuple( generator.getNextTuple() );
    }
    
    readDataAndVerify( operator.getStore(), generator );
  }
  
  public void readDataAndVerify( MemcacheStore store, TupleGenerator<TestPOJO> generator )
  {
    generator.reset();
    
    for( int i=0; i<TUPLE_SIZE; ++i )
    {
      TestPOJO expected = generator.getNextTuple();
      TestPOJO read = (TestPOJO)store.get( expected.getRow() );
      Assert.assertTrue( String.format( "expected={%s}, actually={%s}", expected.toString(), read.toString() ), expected.completeEquals(read) );
    }
  }
}
