/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SimpleQueryValidator implements CustomQueryValidator
{
  private Validator validator;

  public SimpleQueryValidator()
  {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    this.validator = factory.getValidator();
  }

  @Override
  public boolean validate(Query query)
  {
    return validator.validate(query).isEmpty();
  }
}
