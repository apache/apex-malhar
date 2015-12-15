/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.XStreamException;
import com.thoughtworks.xstream.converters.basic.DateConverter;

import com.datatorrent.api.Context;

/**
 * Operator that converts XML string to Pojo <br>
 * <b>Properties</b> <br>
 * <b>alias</b>:This maps to the root element of the XML string. If not
 * specified, parser would expect the root element to be fully qualified name of
 * the Pojo Class. <br>
 * <b>dateFormats</b>: Comma separated string of date formats e.g
 * dd/mm/yyyy,dd-mmm-yyyy where first one would be considered default
 * 
 * @displayName XmlParser
 * @category Parsers
 * @tags xml pojo parser
 * @since 3.2.0
 */
@InterfaceStability.Evolving
public class XmlParser extends Parser<String>
{

  private transient XStream xstream;
  protected String alias;
  protected String dateFormats;

  public XmlParser()
  {
    alias = null;
    dateFormats = null;
  }

  @Override
  public void activate(Context context)
  {
    xstream = new XStream();
    if (alias != null) {
      try {
        xstream.alias(alias, clazz);
      } catch (Throwable e) {
        throw new RuntimeException("Unable find provided class");
      }
    }
    if (dateFormats != null) {
      String[] dateFormat = dateFormats.split(",");
      xstream.registerConverter(new DateConverter(dateFormat[0], dateFormat));
    }
  }

  @Override
  public void deactivate()
  {

  }

  @Override
  public Object convert(String tuple)
  {
    try {
      return xstream.fromXML(tuple);
    } catch (XStreamException e) {
      logger.debug("Error while converting tuple {} {}", tuple,e.getMessage());
      return null;
    }
  }

  /**
   * Gets the alias
   * 
   * @return alias.
   */
  public String getAlias()
  {
    return alias;
  }

  /**
   * Sets the alias This maps to the root element of the XML string. If not
   * specified, parser would expect the root element to be fully qualified name
   * of the Pojo Class.
   * 
   * @param alias
   *          .
   */
  public void setAlias(String alias)
  {
    this.alias = alias;
  }

  /**
   * Gets the comma separated string of date formats e.g dd/mm/yyyy,dd-mmm-yyyy
   * where first one would be considered default
   * 
   * @return dateFormats.
   */
  public String getDateFormats()
  {
    return dateFormats;
  }

  /**
   * Sets the comma separated string of date formats e.g dd/mm/yyyy,dd-mmm-yyyy
   * where first one would be considered default
   * 
   * @param dateFormats
   *          .
   */
  public void setDateFormats(String dateFormats)
  {
    this.dateFormats = dateFormats;
  }

  private static final Logger logger = LoggerFactory.getLogger(XmlParser.class);

}
