package com.datatorrent.apps.telecom.operator;

import java.util.Map;
import java.util.Properties;

public interface EnricherInterface
{

  /**
   * This is used to configure the enricher
   * @param prop
   */
  public void configure(Properties prop);
  
  /**
   * This is used to update the map object with new values
   * @param m
   */
  public void enrichRecord(Map<String, String> m);
}
