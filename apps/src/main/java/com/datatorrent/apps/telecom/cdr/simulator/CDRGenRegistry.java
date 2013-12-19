package com.datatorrent.apps.telecom.cdr.simulator;


import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;

import com.google.common.base.Function;


public class CDRGenRegistry
{

  final static String[] location = {"London", "Liverpool", "New Castle", "Manchester", "Birmingham", "Bristol", "Cambridge", "Leeds"};
  
  final static SimpleDateFormat[] sdfs = {new SimpleDateFormat("dd/MM/yyyy"), new SimpleDateFormat()};
  
  public static class UK_Standard_2012 { 
    public static Object[] dataTemplate = {
    new String[]{"V", "VOIP", "D", "C", "N", "I", "U", "B", "X", "M", "G"},
    new String[]{"0","1"}, new Object[]{digits, 11}, new Object[]{digits, 11}, new Object[]{currentTime, "dd/MM/yyyy"}, 
    new Object[]{currentTime, "HH:mm:ss"}, Range.uniform(0, 600), "", "", location, "UK Local", 
    new String[]{"Peak", "OffPeak", "Weekend"}, Range.uniform(0, 20), Range.uniform(0, 30), 
    new Object[]{digits, 4}, new Object[]{digits, 11}, "", new String[]{"BT313OACA", "BT312CR", "BT313RCCA"},"", new String[]{"", "0", "1"}, 
    new String[]{"S", "Z"}, "", "", "", "", "", new Object[]{digits, 11}, Range.uniform(0, 100), new Object[]{timedIdGen, null}};
  }
  
  static Function<String, String> timedIdGen = new Function<String, String>() {
    
    @Override
    public String apply(String input)
    {
      return Long.toHexString(System.nanoTime());
    }
  };
  
  static Function<Integer, String> digits = new Function<Integer, String>() {
     
    @Override
    public String apply(Integer input)
    {
      return RandomStringUtils.randomNumeric(input);
    }
  };
  
  static Function<String, String> currentTime = new Function<String, String>() {
    
    final SimpleDateFormat sdf = new SimpleDateFormat();
    
    @Override
    public String apply(String pattern)
    {
      sdf.applyPattern(pattern);
      return sdf.format(new Date());
    }
  };
  
  static class Range
  {
    private final RandomDataGenerator gen = new RandomDataGenerator();
    
    private double[] numberKept = new double[5];
    
    static enum Algo{
      gamma, uniform
    }
    
    private Algo a = null;
    
    private Range(Algo a){
      this.a = a;
    };
    
    public static Range gamma(double shape, double scale){
      Range r = new Range(Algo.gamma);
      r.numberKept[0] = shape; r.numberKept[1] = scale;
      return r;
    }
    
    public static Range uniform(double lower, double upper){
      Range r = new Range(Algo.uniform);
      r.numberKept[0] = lower; r.numberKept[1] = upper;
      return r;
    }
    
    @Override
    public String toString()
    {
      switch (a) {
      case gamma:
        return String.valueOf(gen.nextGamma(numberKept[0], numberKept[1]));
      case uniform:
        return String.valueOf(gen.nextUniform(numberKept[0], numberKept[1]));
      default:
        return "";
      }
    }

  }

  
}
