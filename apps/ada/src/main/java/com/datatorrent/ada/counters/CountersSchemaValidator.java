/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.datatorrent.ada.counters.CountersSchema.CountersSchemaValues;
import com.datatorrent.lib.appdata.qr.CustomDataValidator;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.SimpleDataValidator;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class CountersSchemaValidator implements CustomDataValidator
{
  private static final Logger logger = LoggerFactory.getLogger(CountersSchemaValidator.class);

  private SimpleDataValidator sdv = new SimpleDataValidator();

  public CountersSchemaValidator()
  {
  }

  @Override
  public boolean validate(Data query)
  {
    if(!sdv.validate(query)) {
      return false;
    }

    CountersSchema cs = (CountersSchema) query;

    if(!GPOUtils.typeMapValidator(cs.getKeys())) {
      return false;
    }

    List<CountersSchemaValues> schemaValues = cs.getValues();

    if(schemaValues.isEmpty()) {
      logger.error("No schema values specified.");
      return false;
    }

    for(CountersSchemaValues csvs: schemaValues) {
      if(!validateSchemaValues(csvs)) {
        return false;
      }
    }

    return true;
  }

  private boolean validateSchemaValues(CountersSchemaValues csvs)
  {
    for(String aggregation: csvs.getAggregations()) {
      if(!CountersSchema.AGGREGATIONS.contains(aggregation)) {
        logger.error("{} is not a valid aggregation. " +
                     "The following are valid aggregations: {}",
                     aggregation,
                     CountersSchema.AGGREGATIONS);
        return false;
      }
    }

    return true;
  }

  private boolean validateAggregation()
  {
    return true;
  }
}
