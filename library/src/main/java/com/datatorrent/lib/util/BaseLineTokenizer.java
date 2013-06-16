/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import javax.validation.constraints.NotNull;

/**
 *
 * Base class for splitting lines into tokens and tokens into sub-tokens. Base class for line split operators.<br>
 * processToken, and processSubToken are called for each token. Users should override calls backs to intercept at any level.<p>
 * This operator is a base class for pass through operators<br>
 * <br>
 * Ideal for applications like word count
 * Ports:<br>
 * <b>data</b>: expects String<br>
 *
 * <b>Benchmarks</b>: None done as this operator is for other operators to override and this operator does not emit any tuple<br>
 *
 * @author amol<br>
 *
 */
public class BaseLineTokenizer extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>(this)
  {
    /**
     * Processes tuples if validTuple returns true
     * Calls: beginProcessTokens, processTokens, endProcessTokens
     */
    @Override
    public void process(String tuple)
    {
      if (!validTuple(tuple)) { // emit error token?
        return;
      }
      beginProcessTokens();
      processTokens(tuple.split(splitBy));
      endProcessTokens();
    }
  };

  @NotNull()
  String splitBy = ";\t";
  String splitTokenBy = "";


  /**
   * getter function for splitBy
   * @return splitBy
   */
  @NotNull()
  public String getSplitBy()
  {
    return splitBy;
  }

  /**
   * getter function for splitTokeBy
   * @return splitTokenBy
   */
  public String getSplitTokenBy()
  {
    return splitTokenBy;
  }

  /**
   * getter function for splitTokeBy
   * @return splitTokenBy
   */
  public boolean hasSplitTokenBy()
  {
    return !splitTokenBy.isEmpty();
  }

  /**
   * setter function for splitBy
   * @param str
   */
  public void setSplitBy(String str)
  {
    splitBy = str;
  }

  /**
   * setter function for splitTokenBy
   * @param str
   */
  public void setSplitTokenBy(String str)
  {
    splitTokenBy = str;
  }

  /**
   * Called at the start of token processing
   */
  public void beginProcessTokens()
  {
  }

  /**
   * Called at the end of token processing
   */
  public void endProcessTokens()
  {
  }

  /**
   * Called at the start of sub token processing
   */
  public void beginProcessSubTokens()
  {
  }

  /**
   * Called at the end of sub token processing
   */
  public void endProcessSubTokens()
  {
  }

  /**
   * Processed each token one at a time in the order received if it is not null and if validToken returns true
   * @param tokens
   */
  public void processTokens(String[] tokens)
  {
    if (tokens == null) {
      return;
    }
    for (String tok: tokens) {
      if (validToken(tok)) {
        processToken(tok);
      }
    }
  }

  /**
   * Processes token
   * @param tok
   */
  public void processToken(String tok)
  {
    if (tok.isEmpty()) {
      return;
    }
    beginProcessSubTokens();
    if (splitTokenBy.isEmpty()) {
        processSubToken(tok);
    }
    else {
      String[] subtoks = tok.split(splitTokenBy);
      int i = 0;
      for (String subtok: subtoks) {
        if ((i ==0) && !validSubTokenKey(subtok)) { // first subtoken is the key
          break;
        }
        processSubToken(subtok);
        i++;
      }
    }
    endProcessSubTokens();
  }

  /**
   * Called for processing subtoken
   * @param subtok
   */
  public void processSubToken(String subtok)
  {
  }

  /**
   *
   * @param tuple
   * @return true is tuple is not empty
   */
  public boolean validTuple(String tuple)
  {
    return !tuple.isEmpty();
  }

  /**
   *
   * @param tok
   * @return true is tok is not empty
   */
  public boolean validToken(String tok)
  {
    return !tok.isEmpty();
  }

  /**
   *
   * @param subtok
   * @return true is subtok is not empty
   */
  public boolean validSubTokenKey(String subtok)
  {
    return !subtok.isEmpty();
  }
}
