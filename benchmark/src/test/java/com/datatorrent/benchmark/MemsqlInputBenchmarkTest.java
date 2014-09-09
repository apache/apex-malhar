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

package com.datatorrent.benchmark;

import com.datatorrent.api.LocalMode;
import org.junit.Test;

public class MemsqlInputBenchmarkTest
{
  @Test
  public void testMethod() {
    MemsqlInputBenchmark app = new MemsqlInputBenchmark();
    app.host = MemsqlOutputBenchmarkTest.HOST;
    LocalMode.runApp(app, 200000);
  }
}
