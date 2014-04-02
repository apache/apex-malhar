/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.etl;

import javax.annotation.Nonnull;

import com.datatorrent.lib.datamodel.operation.Operation;
import java.io.Serializable;

public class Metric implements Serializable
{
  @Nonnull
  String sourceKey;
  String destinationKey;
  Operation operation;

  public Metric()
  {
    // for serialization
  }


  public  Metric(@Nonnull String sourceKey, @Nonnull String destinationKey, @Nonnull Operation operation)
  {
    this.sourceKey = sourceKey;
    this.destinationKey = destinationKey;
    this.operation = operation;
  }
}
