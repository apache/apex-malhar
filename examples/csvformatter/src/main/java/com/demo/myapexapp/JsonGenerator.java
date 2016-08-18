package com.demo.myapexapp;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import javax.validation.constraints.Min;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class JsonGenerator extends BaseOperator implements InputOperator
{

  private static final Logger LOG = LoggerFactory.getLogger(JsonGenerator.class);

  @Min(1)
  private int numTuples = 20;
  private transient int count = 0;

  public static Random rand = new Random();
  private int sleepTime=5;

  public final transient DefaultOutputPort<byte[]> out = new DefaultOutputPort<byte[]>();

  private static String getJson()
  {

    JSONObject obj = new JSONObject();
    try {
      obj.put("campaignId", 1234);
      obj.put("campaignName", "SimpleCsvFormatterExample");
      obj.put("campaignBudget", 10000.0);
      obj.put("weatherTargeting", "false");
      obj.put("securityCode", "APEX");
    } catch (JSONException e) {
      return null;
    }
    return obj.toString();
  }

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      out.emit(getJson().getBytes());
    } else {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.info("Sleep interrupted");
      }
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }

}
