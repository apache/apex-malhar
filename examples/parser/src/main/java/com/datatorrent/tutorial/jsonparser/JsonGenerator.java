package com.datatorrent.tutorial.jsonparser;

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
  public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
  public static int[] adId = { 1, 2, 3, 4, 5 };
  public static String[] campaignName = { "cmp1", "cmp2", "cmp3", "cmp4" };
  public static double[] campaignBudget = { 10000.0, 20000.0, 300000.0 };
  public static boolean[] weatherTargeting = { true, false };
  private int sleepTime;

  public final transient DefaultOutputPort<byte[]> out = new DefaultOutputPort<byte[]>();

  private static String getNext(int num)
  {

    JSONObject obj = new JSONObject();
    try {
      obj.put("adId", adId[num % adId.length]);
      obj.put("campaignName", campaignName[num % campaignName.length]);
      obj.put("campaignBudget", campaignBudget[num % campaignBudget.length]);
      obj.put("weatherTargeting", weatherTargeting[num % weatherTargeting.length]);
      obj.put("startDate", sdf.format(new Date()));
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
      out.emit(getNext(rand.nextInt(numTuples) + 1).getBytes());
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
