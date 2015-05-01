/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @displayName Simple Pojo Dimensions Computation
 * @category Statistics
 * @tags event, dimension, aggregation, computation, pojo
 */
public class DimensionsComputationSingleSchemaPOJO extends DimensionsComputationSingleSchemaConv<Object, DimensionsPOJOConverter>
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsComputationSingleSchemaPOJO.class);

  public DimensionsComputationSingleSchemaPOJO()
  {
    this.converter = new DimensionsPOJOConverter();
  }

  @Override
  public void setup(OperatorContext context)
  {
    logger.debug("Setup called");
    super.setup(context);

    converter.getPojoFieldRetriever().setFieldToType(SchemaUtils.convertFieldToType(eventSchema.getAllFieldToType()));
    converter.getPojoFieldRetriever().setup();
  }
}
