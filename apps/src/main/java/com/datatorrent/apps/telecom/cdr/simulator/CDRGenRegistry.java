/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.telecom.cdr.simulator;


import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;

import com.google.common.base.Function;


/**
 * This class contains all the self-declarative template used by CDRSimulator to generate CDR records.
 * 
 * Each template is a class contain a dataTemplate Object array. Each object in the array is a template for the value of certain cell
 * 
 * The object is the array could be either 
 * 1 Another object array as a enum that generator will randomly pick one from it
 * 2 A 2-element array with a Function object(https://code.google.com/p/guava-libraries/wiki/FunctionalExplained) and it's parameter. The generator will call Function.apply(param) method to generate the value
 * 3 Any other arbitrary object. The generator will call toString method to generate the value
 */
public class CDRGenRegistry
{

  final static String[] location = {"London", "Liverpool", "New Castle", "Manchester", "Birmingham", "Bristol", "Cambridge", "Leeds"};
  
  public static class UK_Standard_2012 { 
    public static Object[] dataTemplate = {
    new String[]{"V", "VOIP", "D", "C", "N", "I", "U", "B", "X", "M", "G"},
    new String[]{"0","1"}, new Object[]{digits, 11}, new Object[]{digits, 11}, new Object[]{currentTime, "dd/MM/yyyy"}, 
    new Object[]{currentTime, "HH:mm:ss"}, Range.uniform(0, 600), "", "", location, "UK Local", 
    new String[]{"Peak", "OffPeak", "Weekend"}, Range.uniform(0, 20), Range.uniform(0, 30), 
    new Object[]{digits, 4}, new Object[]{digits, 11}, "", new String[]{"BT313OACA", "BT312CR", "BT313RCCA"},"", new String[]{"", "0", "1"}, 
    new String[]{"S", "Z"}, "", "", "", "", "", new Object[]{digits, 11}, Range.uniform(0, 100), new Object[]{timedIdGen, null}};
  }
  
  /**
   * A declarative object to generate the unique id by system time. Usage: new Object[]{timedIdGen, null}
   */
  static Function<String, String> timedIdGen = new Function<String, String>() {
    
    @Override
    public String apply(String input)
    {
      return Long.toHexString(System.nanoTime());
    }
  };
  
  
  /**
   * A declarative object to generate string of digits at certain length. Usage: new Object[]{digits, 10} (10 digits string)
   */
  static Function<Integer, String> digits = new Function<Integer, String>() {
     
    @Override
    public String apply(Integer input)
    {
      return RandomStringUtils.randomNumeric(input);
    }
  };
  
  /**
   * A declarative object to generate string of date using certain date format. Usage: new Object[]{currentTime, "dd/MM/yyyy"} ("dd/MM/yyyy" is output pattern)
   */
  static Function<String, String> currentTime = new Function<String, String>() {
    
    final SimpleDateFormat sdf = new SimpleDateFormat();
    
    @Override
    public String apply(String pattern)
    {
      sdf.applyPattern(pattern);
      return sdf.format(new Date());
    }
  };
  
  /**
   * A range class generate numbers conforming to some distribution (uniform, gamma) 
   */
  static class Range
  {
    private final RandomDataGenerator gen = new RandomDataGenerator();
    
    // Reserve  5 number parameters for different random algorithms 
    private double[] numberKept = new double[5];
    
    static enum Algo{
      gamma, uniform
    }
    
    private Algo a = null;
    
    private Range(Algo a){
      this.a = a;
    };
    
    
    /**
     * Build a Range which can randomly generate numbers in Gamma distribution
     * @param shape
     * @param scale
     * @return
     */
    public static Range gamma(double shape, double scale){
      Range r = new Range(Algo.gamma);
      r.numberKept[0] = shape; r.numberKept[1] = scale;
      return r;
    }
    
    /**
     * Build a Range which can randomly generate numbers in uniform distribution between lower and upper
     * @param lower
     * @param upper
     * @return
     */
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
