package com.datatorrent.apps.telecom.util;

import java.util.Random;

public class RandomString
{

  private static final char[] symbols = new char[10];

  static {
    for (int idx = 0; idx < 10; ++idx)
      symbols[idx] = (char) ('0' + idx);    
  }

  private final static Random random = new Random();

  public static String nextString(int length)
  {
    char[] buf = new char[length];
    for (int idx = 0; idx < buf.length; ++idx) 
      buf[idx] = symbols[random.nextInt(symbols.length)];
    return new String(buf);
  }

}