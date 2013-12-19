package com.datatorrent.apps.telecom.cdr.simulator;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class TypePartitioner implements Partitioner<String>
{

  public TypePartitioner (VerifiableProperties props) {
    
  }
  @Override
  public int partition(String key, int pNum)
  {
    if(key.equalsIgnoreCase("U") || key.equalsIgnoreCase("V"))
      return 0;
    else return 1;
  }

}
