/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.goldengate;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KafkaJsonEncoder implements kafka.serializer.Encoder<Object>
{
  private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonEncoder.class);
  private final ObjectMapper mapper = new ObjectMapper();

  public KafkaJsonEncoder(kafka.utils.VerifiableProperties props)
  {
  }

  @Override
  public byte[] toBytes(Object arg0)
  {
    try {
      return mapper.writeValueAsBytes(arg0);
    }
    catch (Exception e) {
      LOG.error("Failed to encode {}", arg0, e);
      throw new RuntimeException(e);
    }
  }

}
