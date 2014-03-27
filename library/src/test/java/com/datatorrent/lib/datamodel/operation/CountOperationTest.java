package com.datatorrent.lib.datamodel.operation;

import junit.framework.Assert;

import org.junit.Test;

public class CountOperationTest
{
  
  @Test
  public void testIntType()
  {
    Integer countV = new Integer(0);
    CountOperation<Integer> count = new CountOperation<Integer>();
    countV = count.compute(countV,1);
    countV = count.compute(countV,2);
    Assert.assertEquals(2, countV.intValue());
  }

  @Test
  public void testLongType()
  {
    Long countV = new Long(0);
    CountOperation<Long> count = new CountOperation<Long>();
    countV = count.compute(countV,null);
    countV = count.compute(countV,null);
    Assert.assertEquals(2l, countV.longValue());
  }

  @Test
  public void testDoubleType()
  {
    Double countV = new Double(0);
    CountOperation<Double> count = new CountOperation<Double>();
    countV = count.compute(countV,null);
    countV = count.compute(countV,null);
    Assert.assertEquals(2.0, countV.doubleValue());
  }

  @Test
  public void testFloatType()
  {
    Float countV = new Float(0);
    CountOperation<Float> count = new CountOperation<Float>();
    countV = count.compute(countV,null);
    countV = count.compute(countV,null);
    Assert.assertEquals(2.0f, countV.floatValue());
  }

}
