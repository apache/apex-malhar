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
public class SimpleDataValidator implements CustomDataValidator
{
  private Validator validator;

  public SimpleDataValidator()
  {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    this.validator = factory.getValidator();
  }

  @Override
  public boolean validate(Data data)
  {
    return validator.validate(data).isEmpty();
  }
}
