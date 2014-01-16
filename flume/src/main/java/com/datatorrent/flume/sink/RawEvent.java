/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.flume.sink;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.Slice;

/**
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class RawEvent
{
  public Slice guid;
  public long time;
  public int dimensionsOffset;

  public Slice getGUID()
  {
    return guid;
  }

  public long getTime()
  {
    return time;
  }

  RawEvent()
  {
    /* needed for Kryo serialization */
  }

  private RawEvent rawEventWithGuidAndTime()
  {
    RawEvent rawEvent = new RawEvent();
    rawEvent.guid = guid;
    rawEvent.time = time;
    return rawEvent;
  }

  public static RawEvent from(byte[] row, byte separator)
  {
    final int rowsize = row.length;

    /*
     * Lets get the guid out of the current record
     */
    int sliceLengh = -1;
    while (++sliceLengh < rowsize) {
      if (row[sliceLengh] == separator) {
        break;
      }
    }

    /* skip the next record */
    int i = sliceLengh + 1;
    while (i < rowsize) {
      if (row[i++] == separator) {
        break;
      }
    }

    /* lets parse the date */
    int dateStart = i;
    while (i < rowsize) {
      if (row[i++] == separator) {
        try {
          long time = dateParser.parse(new String(row, dateStart, i - dateStart)).getTime();
          RawEvent event = new RawEvent();
          event.guid = new Slice(row, 0, sliceLengh);
          event.time = time;
          event.dimensionsOffset = i;
          return event;
        }
        catch (ParseException ex) {
          logger.debug("could not process the record {}", row);
        }
      }
    }

    return null;
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 61 * hash + (this.guid != null ? this.guid.hashCode() : 0);
    hash = 61 * hash + (int) (this.time ^ (this.time >>> 32));
    return hash;
  }

  @Override
  public String toString()
  {
    return "RawEvent{" + "guid=" + guid + ", time=" + time + '}';
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final RawEvent other = (RawEvent) obj;
    if (this.guid != other.guid && (this.guid == null || !this.guid.equals(other.guid))) {
      return false;
    }
    return this.time == other.time;
  }

  public Object getEventKey()
  {
    return rawEventWithGuidAndTime();
  }

  static final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final Logger logger = LoggerFactory.getLogger(RawEvent.class);
}
