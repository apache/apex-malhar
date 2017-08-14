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
package org.apache.apex.malhar.contrib.parser;

import java.util.Date;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.appdata.schemas.SchemaUtils;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

public class CsvPOJOParserTest
{

  private static final String filename = "schema.json";
  CollectorTestSink<Object> error = new CollectorTestSink<Object>();
  CollectorTestSink<Object> objectPort = new CollectorTestSink<Object>();
  CollectorTestSink<Object> pojoPort = new CollectorTestSink<Object>();
  CsvParser parser = new CsvParser();

  @Rule
  public Watcher watcher = new Watcher();

  public class Watcher extends TestWatcher
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      parser.setClazz(Ad.class);
      parser.setSchema(SchemaUtils.jarResourceFileToString(filename));
      parser.setup(null);
      parser.err.setSink(error);
      parser.parsedOutput.setSink(objectPort);
      parser.out.setSink(pojoPort);
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      error.clear();
      objectPort.clear();
      pojoPort.clear();
      parser.teardown();
    }
  }

  /*
  * adId,campaignId,adName,bidPrice,startDate,endDate,securityCode,isActive,isOptimized,parentCampaign,weatherTargeted,valid
  * 1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZE,CAMP_AD,Y,yes
  * Constraints are defined in schema.json
  */
  @Test
  public void TestParserValidInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Ad adPojo = (Ad)obj;
    Assert.assertNotNull(obj);
    Assert.assertEquals(Ad.class, obj.getClass());
    Assert.assertEquals(1234, adPojo.getAdId());
    Assert.assertTrue("adxyz".equals(adPojo.getAdName()));
    Assert.assertEquals(0.2, adPojo.getBidPrice(), 0.0);
    Assert.assertEquals(Date.class, adPojo.getStartDate().getClass());
    Assert.assertEquals(Date.class, adPojo.getEndDate().getClass());
    Assert.assertEquals(12, adPojo.getSecurityCode());
    Assert.assertTrue("CAMP_AD".equals(adPojo.getParentCampaign()));
    Assert.assertTrue(adPojo.isActive());
    Assert.assertFalse(adPojo.isOptimized());
    Assert.assertTrue("yes".equals(adPojo.getValid()));
  }

  @Test
  public void TestParserValidInputPojoPortNotConnected()
  {
    parser.out.setSink(null);
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
  }

  @Test
  public void TestParserValidInputClassNameNotProvided()
  {
    parser.setClazz(null);
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidAdIdInput()
  {
    String input = ",98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserNoCampaignIdInput()
  {
    String input = "1234,,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(Ad.class, obj.getClass());
    Assert.assertEquals(0, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidCampaignIdInput()
  {
    String input = "1234,9833,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserInvalidAdNameInput()
  {
    String input = "1234,98233,adxyz123,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserInvalidBidPriceInput()
  {
    String input = "1234,98233,adxyz,3.3,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserInvalidStartDateInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-30-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserInvalidSecurityCodeInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,85,y,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserInvalidisActiveInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,yo,,CAMP_AD,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserInvalidParentCampaignInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserValidisOptimized()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZE,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(Ad.class, obj.getClass());
    Assert.assertEquals(0, error.collectedTuples.size());
  }

  @Test
  public void TestParserInValidisOptimized()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZATION,CAMP,Y,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserInValidWeatherTargeting()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZE,CAMP_AD,NO,yes";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserNullOrBlankInput()
  {
    parser.beginWindow(0);
    parser.in.process(null);
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserHeaderAsInput()
  {
    String input = "adId,campaignId,adName,bidPrice,startDate,endDate,securityCode,active,optimized,parentCampaign,weatherTargeted,valid";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserLessFields()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZATION";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String,String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserLessFieldsOnlyPOJOPortConnected()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZATION";
    parser.parsedOutput.setSink(null);
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String, String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserMoreFields()
  {

    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZATION,CAMP_AD,Y,yes,ExtraField";
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String, String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserMoreFieldsOnlyPOJOPortConnected()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZATION,CAMP_AD,Y,yes,ExtraField";
    parser.parsedOutput.setSink(null);
    parser.beginWindow(0);
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String, String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(input, errorTuple.getKey());
  }

  @Test
  public void TestParserValidInputMetricVerification()
  {
    parser.beginWindow(0);
    Assert.assertEquals(0, parser.parsedOutputCount);
    Assert.assertEquals(0, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(0, parser.getEmittedObjectCount());
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, parser.parsedOutputCount);
    Assert.assertEquals(1, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(1, parser.getEmittedObjectCount());
  }

  @Test
  public void TestParserInvalidInputMetricVerification()
  {
    parser.beginWindow(0);
    Assert.assertEquals(0, parser.parsedOutputCount);
    Assert.assertEquals(0, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(0, parser.getEmittedObjectCount());
    parser.in.process("1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZATION,CAMP_AD,Y,yes,ExtraField"
        .getBytes());
    parser.endWindow();
    Assert.assertEquals(0, parser.parsedOutputCount);
    Assert.assertEquals(1, parser.getIncomingTuplesCount());
    Assert.assertEquals(1, parser.getErrorTupleCount());
    Assert.assertEquals(0, parser.getEmittedObjectCount());
  }

  @Test
  public void TestParserValidInputMetricResetCheck()
  {
    parser.beginWindow(0);
    Assert.assertEquals(0, parser.parsedOutputCount);
    Assert.assertEquals(0, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(0, parser.getEmittedObjectCount());
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.endWindow();
    Assert.assertEquals(1, parser.parsedOutputCount);
    Assert.assertEquals(1, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(1, parser.getEmittedObjectCount());
    parser.beginWindow(1);
    Assert.assertEquals(0, parser.parsedOutputCount);
    Assert.assertEquals(0, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(0, parser.getEmittedObjectCount());
    parser.in.process(input.getBytes());
    Assert.assertEquals(1, parser.parsedOutputCount);
    Assert.assertEquals(1, parser.getIncomingTuplesCount());
    Assert.assertEquals(0, parser.getErrorTupleCount());
    Assert.assertEquals(1, parser.getEmittedObjectCount());
    parser.endWindow();
  }

  public static class Ad
  {

    private int adId;
    private int campaignId;
    private String adName;
    private double bidPrice;
    private Date startDate;
    private Date endDate;
    private long securityCode;
    private boolean active;
    private boolean optimized;
    private String parentCampaign;
    private Character weatherTargeted;
    private String valid;

    public Ad()
    {

    }

    public int getAdId()
    {
      return adId;
    }

    public void setAdId(int adId)
    {
      this.adId = adId;
    }

    public int getCampaignId()
    {
      return campaignId;
    }

    public void setCampaignId(int campaignId)
    {
      this.campaignId = campaignId;
    }

    public String getAdName()
    {
      return adName;
    }

    public void setAdName(String adName)
    {
      this.adName = adName;
    }

    public double getBidPrice()
    {
      return bidPrice;
    }

    public void setBidPrice(double bidPrice)
    {
      this.bidPrice = bidPrice;
    }

    public Date getStartDate()
    {
      return startDate;
    }

    public void setStartDate(Date startDate)
    {
      this.startDate = startDate;
    }

    public Date getEndDate()
    {
      return endDate;
    }

    public void setEndDate(Date endDate)
    {
      this.endDate = endDate;
    }

    public long getSecurityCode()
    {
      return securityCode;
    }

    public void setSecurityCode(long securityCode)
    {
      this.securityCode = securityCode;
    }

    public boolean isActive()
    {
      return active;
    }

    public void setActive(boolean active)
    {
      this.active = active;
    }

    public boolean isOptimized()
    {
      return optimized;
    }

    public void setOptimized(boolean optimized)
    {
      this.optimized = optimized;
    }

    public String getParentCampaign()
    {
      return parentCampaign;
    }

    public void setParentCampaign(String parentCampaign)
    {
      this.parentCampaign = parentCampaign;
    }

    public Character getWeatherTargeted()
    {
      return weatherTargeted;
    }

    public void setWeatherTargeted(Character weatherTargeted)
    {
      this.weatherTargeted = weatherTargeted;
    }

    public String getValid()
    {
      return valid;
    }

    public void setValid(String valid)
    {
      this.valid = valid;
    }

    @Override
    public String toString()
    {
      return "Ad [adId=" + adId + ", campaignId=" + campaignId + ", adName=" + adName + ", bidPrice=" + bidPrice
          + ", startDate=" + startDate + ", endDate=" + endDate + ", securityCode=" + securityCode + ", active="
          + active + ", optimized=" + optimized + ", parentCampaign=" + parentCampaign + ", weatherTargeted="
          + weatherTargeted + ", valid=" + valid + "]";
    }
  }

}
