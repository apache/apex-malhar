/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.datatorrent.lib.appdata.qr.CustomDataValidator;
import com.datatorrent.lib.appdata.qr.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class CountersUpdateDataLogicalValidator implements CustomDataValidator
{
  private static final Logger logger = LoggerFactory.getLogger(CountersUpdateDataLogicalValidator.class);

  @Override
  public boolean validate(Data query)
  {
    CountersUpdateDataLogical cud = (CountersUpdateDataLogical) query;

    CountersSchema schema = CountersSchemaUtils.getSchema(cud);

    //Schema wasn't received yet so invalid counter.
    if(schema == null) {
      return false;
    }

    return true;
  }
}
