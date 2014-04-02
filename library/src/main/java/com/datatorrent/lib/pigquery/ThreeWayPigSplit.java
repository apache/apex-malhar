/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.pigquery;

import java.util.Map;

/**
 * <p>
 * Class to implement Pig split operator semantic. A split into node creates
 * multiple streams from a single stream. <br>
 * This operator implements following semantic.  <br>
 * <pre>
 * instream is f1:int, f2:int, f3:int
 *
 * The expression for split into is "X IF f1<7, Y IF f2==5, Z IF (f3<6)"
 *
 * For instream tuples (1,2,3) would produce (1,2,3) on X, (1,2,3) on Z (4,5,6)
 * would produce (4,5,6) on X, (4,5,6) on Y (7,8,9) would produce (7,8,9) on Z
 *
 * This would match to split into metric for Pig
 * </pre>
 *
 * @since 0.3.4
 */
public class ThreeWayPigSplit  extends PigSplitOperator<Map<String, Integer>>
{

  public ThreeWayPigSplit()
  {
    super(3);
  }

  @Override
  public boolean isValidEmit(int i, Map<String, Integer> tuple)
  {
    if (i == 0) {
      if (tuple.get("f1") < 7) return true;
    }
    if (i == 1) {
      if (tuple.get("f2") == 5) return true;
    }
    if (i == 2) {
      if (tuple.get("f3") < 6) return true;
    }
    return false;
  }

}
