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
package com.datatorrent.contrib.parser;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.netlet.util.DTThrowable;

public class CsvPOJOParserTest
{

  private static final String filename = "schema.json";
  CollectorTestSink<Object> error = new CollectorTestSink<Object>();
  CollectorTestSink<Object> objectPort = new CollectorTestSink<Object>();
  CollectorTestSink<Object> pojoPort = new CollectorTestSink<Object>();
  CsvParser parser = new CsvParser();

  @ClassRule
  public static TestMeta testMeta = new TestMeta();

  public static class TestMeta extends TestWatcher
  {
    public org.junit.runner.Description desc;
    public String dirName;

    @Override
    protected void starting(Description description)
    {
      this.desc = description;
      super.starting(description);
      dirName = "target/" + desc.getClassName();
      new File(dirName).mkdir();
      FileSystem fileSystem = null;
      //Create file
      Path newFilePath = new Path(dirName + "/" + filename);
      try {
        fileSystem = LocalFileSystem.get(new Configuration());
        fileSystem.createNewFile(newFilePath);
      } catch (IOException ex) {
        DTThrowable.rethrow(ex);
      }
      //Writing contents to file
      StringBuilder sb = new StringBuilder();
      sb.append(SchemaUtils.jarResourceFileToString(filename));
      byte[] byt = sb.toString().getBytes();
      try {
        FSDataOutputStream fsOutStream = fileSystem.create(newFilePath);
        fsOutStream.write(byt);
        fsOutStream.close();
      } catch (IOException ex) {
        DTThrowable.rethrow(ex);
      }
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(dirName));
    }
  }

  /*
  * adId,campaignId,adName,bidPrice,startDate,endDate,securityCode,isActive,isOptimized,parentCampaign,weatherTargeted,valid
  * 1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZE,CAMP_AD,Y,yes
  * Constraints are defined in schema.json
  */

  @Before
  public void setup()
  {
    parser.err.setSink(error);
    parser.parsedOutput.setSink(objectPort);
    parser.out.setSink(pojoPort);
    parser.setClazz(Ad.class);
    parser.setSchemaPath(testMeta.dirName + "/" + filename);
    parser.setup(null);
  }

  @After
  public void tearDown()
  {
    error.clear();
    objectPort.clear();
    pojoPort.clear();
  }

  @Test
  public void TestParserValidInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
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
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
  }

  @Test
  public void TestParserValidInputClassNameNotProvided()
  {
    parser.setClazz(null);
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidAdIdInput()
  {
    String input = ",98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserNoCampaignIdInput()
  {
    String input = "1234,,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
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
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidAdNameInput()
  {
    String input = "1234,98233,adxyz123,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidBidPriceInput()
  {
    String input = "1234,98233,adxyz,3.3,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidStartDateInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-30-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidSecurityCodeInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,85,y,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidisActiveInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,yo,,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInvalidParentCampaignInput()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,,CAMP,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserValidisOptimized()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZE,CAMP_AD,Y,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
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
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserInValidWeatherTargeting()
  {
    String input = "1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZE,CAMP_AD,NO,yes";
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserNullOrBlankInput()
  {
    parser.in.process(null);
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserHeaderAsInput()
  {
    String input = "adId,campaignId,adName,bidPrice,startDate,endDate,securityCode,active,optimized,parentCampaign,weatherTargeted,valid";
    parser.in.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserLessFields()
  {
    parser.in.process("1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZATION".getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestParserMoreFields()
  {
    parser.in.process("1234,98233,adxyz,0.2,2015-03-08 03:37:12,11/12/2012,12,y,OPTIMIZATION,CAMP_AD,Y,yes,ExtraField"
        .getBytes());
    parser.teardown();
    Assert.assertEquals(0, objectPort.collectedTuples.size());
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
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
