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

package com.datatorrent.lib.appdata.schemas;

import java.text.DecimalFormat;
import javax.validation.constraints.NotNull;
import jline.internal.Preconditions;

public class AppDataFormatter
{
  private String decimalFormatString;
  private transient DecimalFormat decimalFormat;

  public AppDataFormatter()
  {
  }

  public DecimalFormat getDecimalFormat()
  {
    if(decimalFormat == null) {
      if(decimalFormatString != null) {
        decimalFormat = new DecimalFormat(decimalFormatString);
      }
      else {
        decimalFormat = new DecimalFormat();
      }
    }

    return decimalFormat;
  }

  /**
   * @return the decimalFormatString
   */
  public String getDecimalFormatString()
  {
    return decimalFormatString;
  }

  /**
   * @param decimalFormatString the decimalFormatString to set
   */
  public void setDecimalFormatString(@NotNull String decimalFormatString)
  {
    this.decimalFormatString = Preconditions.checkNotNull(decimalFormatString);
  }
}
