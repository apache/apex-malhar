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
package org.apache.apex.malhar.lib.math;

import com.datatorrent.api.annotation.Stateless;

/**
 * Emits the result as square of the input tuple which is a number.
 * <p>
 * Emits the result as Long on port longResult, as Integer on port integerResult,
 * as Double on port doubleResult, and as Float on port floatResult.
 * This is a pass through operator
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Number<br>
 * <b>longResult</b>: emits Long<br>
 * <b>integerResult</b>: emits Integer<br>
 * <b>doubleResult</b>: emits Double<br>
 * <b>floatResult</b>: emits Float<br>
 * <br>
 * @displayName Square Calculus
 * @category Math
 * @tags  numeric, square, multiplication
 * @since 0.3.3
 */
@Stateless
public class SquareCalculus extends SingleVariableAbstractCalculus
{
  @Override
  public double function(double dval)
  {
    return dval * dval;
  }

  @Override
  public long function(long lval)
  {
    return lval * lval;
  }

}
