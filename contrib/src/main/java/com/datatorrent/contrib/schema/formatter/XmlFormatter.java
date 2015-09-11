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
package com.datatorrent.contrib.schema.formatter;

import java.io.Writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.XStreamException;
import com.thoughtworks.xstream.converters.basic.DateConverter;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.xml.CompactWriter;
import com.thoughtworks.xstream.io.xml.XppDriver;

/**
 * @displayName XmlParser
 * @category Formatter
 * @tags xml pojo formatter
 */
@InterfaceStability.Evolving
public class XmlFormatter extends Formatter<String>
{

  private transient XStream xstream;

  protected String alias;
  protected String dateFormat;
  protected boolean prettyPrint;

  public XmlFormatter()
  {
    alias = null;
    dateFormat = null;
  }

  @Override
  public void activate(Context context)
  {
    if (prettyPrint) {
      xstream = new XStream();
    } else {
      xstream = new XStream(new XppDriver()
      {
        @Override
        public HierarchicalStreamWriter createWriter(Writer out)
        {
          return new CompactWriter(out, getNameCoder());
        }
      });
    }
    if (alias != null) {
      try {
        xstream.alias(alias, clazz);
      } catch (Throwable e) {
        throw new RuntimeException("Unable find provided class");
      }
    }
    if (dateFormat != null) {
      xstream.registerConverter(new DateConverter(dateFormat, new String[] {}));
    }
  }

  @Override
  public void deactivate()
  {

  }

  @Override
  public String convert(Object tuple)
  {
    try {
      return xstream.toXML(tuple);
    } catch (XStreamException e) {
      logger.debug("Error while converting tuple {} {} ",tuple,e.getMessage());
      return null;
    }
  }

  /**
   * Gets the alias This is an optional step. Without it XStream would work
   * fine, but the XML element names would contain the fully qualified name of
   * each class (including package) which would bulk up the XML a bit.
   * 
   * @return alias.
   */
  public String getAlias()
  {
    return alias;
  }

  /**
   * Sets the alias This is an optional step. Without it XStream would work
   * fine, but the XML element names would contain the fully qualified name of
   * each class (including package) which would bulk up the XML a bit.
   * 
   * @param alias
   *          .
   */
  public void setAlias(String alias)
  {
    this.alias = alias;
  }

  /**
   * Gets the date format e.g dd/mm/yyyy - this will be how a date would be
   * formatted
   * 
   * @return dateFormat.
   */
  public String getDateFormat()
  {
    return dateFormat;
  }

  /**
   * Sets the date format e.g dd/mm/yyyy - this will be how a date would be
   * formatted
   * 
   * @param dateFormat
   *          .
   */
  public void setDateFormat(String dateFormat)
  {
    this.dateFormat = dateFormat;
  }

  /**
   * Returns true if pretty print is enabled.
   * 
   * @return prettyPrint
   */
  public boolean isPrettyPrint()
  {
    return prettyPrint;
  }

  /**
   * Sets pretty print option.
   * 
   * @param prettyPrint
   */
  public void setPrettyPrint(boolean prettyPrint)
  {
    this.prettyPrint = prettyPrint;
  }

  private static final Logger logger = LoggerFactory.getLogger(XmlFormatter.class);

}
