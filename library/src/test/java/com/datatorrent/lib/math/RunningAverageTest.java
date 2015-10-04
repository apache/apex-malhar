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
package com.datatorrent.lib.math;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional tests for {@link com.datatorrent.lib.math.RunningAverage}
 */
public class RunningAverageTest
{
	public RunningAverageTest()
	{
	}

	@Test
	public void testLogicForSmallValues()
	{
		logger.debug("small values");
		RunningAverage instance = new RunningAverage();
		instance.input.process(1.0);

		assertEquals("first average", 1.0, instance.average, 0.00001);
		assertEquals("first count", 1, instance.count);

		instance.input.process(2.0);

		assertEquals("second average", 1.5, instance.average, 0.00001);
		assertEquals("second count", 2, instance.count);
	}

	@Test
	public void testLogicForLargeValues()
	{
		logger.debug("large values");
		RunningAverage instance = new RunningAverage();
		instance.input.process(Long.MAX_VALUE);

		assertEquals("first average", Long.MAX_VALUE, (long) instance.average);

		instance.input.process(Long.MAX_VALUE);
		assertEquals("second average", Long.MAX_VALUE, (long) instance.average);
	}

	private static final Logger logger = LoggerFactory
			.getLogger(RunningAverageTest.class);
}
