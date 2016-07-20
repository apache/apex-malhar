package com.example.myapexapp;

import java.util.Random;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Generates Subscriber Data:
 *    A Party Phone
 *    A Party IMEI
 *    A Party IMSI
 *    Circle Id
 */
public class DataGenerator extends BaseOperator implements InputOperator
{
  public static int NUM_CIRCLES = 10;

  private Random r;
  private int count = 0;
  private int limit = 1000;

  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<>();

  @Override
  public void setup(OperatorContext context)
  {
    r = new Random(System.currentTimeMillis());
  }

  @Override
  public void beginWindow(long windowId) {
    super.beginWindow(windowId);
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if(count++ < limit) {
      output.emit(getRecord());
    }
  }

  private byte[] getRecord()
  {
    String phone = getRandomNumber(10);
    String imsi = getHashInRange(phone, 15);
    String imei = getHashInRange(imsi, 15);
    String circleId = Math.abs(phone.hashCode()) % NUM_CIRCLES + "";
//    String record = MessageFormat.format(baseDataTemplate, phone, imsi, imei, circleId);
    String record = "{" +
                    "\"phone\":\"" + phone + "\"," +
                    "\"imei\":\"" + imei+ "\"," +
                    "\"imsi\":\"" + imsi+ "\"," +
                    "\"circleId\":" + circleId +
                    "}";
    return record.getBytes();
  }

  private String getRandomNumber(int numDigits)
  {
    String retVal = (r.nextInt((9 - 1) + 1) + 1) + "";

    for (int i = 0; i < numDigits - 1; i++) {
      retVal += (r.nextInt((9 - 0) + 1) + 0);
    }
    return retVal;
  }

  private String getHashInRange(String s, int n)
  {
    StringBuilder retVal = new StringBuilder();
    for (int i = 0, j = 0; i < n && j < s.length(); i++, j++) {
      retVal.append(Math.abs(s.charAt(j) + "".hashCode()) % 10);
      if (j == s.length() - 1) {
        j = -1;
      }
    }
    return retVal.toString();
  }

  public int getLimit()
  {
    return limit;
  }

  public void setLimit(int limit)
  {
    this.limit = limit;
  }
}
