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
package com.datatorrent.demos.mroperator;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.demos.mroperator.ReporterImpl.ReporterType;
import com.datatorrent.lib.util.KeyHashValPair;

/**
 * <p>ReduceOperator class.</p>
 *
 * @since 0.9.0
 */
@SuppressWarnings({ "deprecation", "unused" })
public class ReduceOperator<K1, V1, K2, V2> implements Operator {
	private static final Logger logger = LoggerFactory.getLogger(ReduceOperator.class);

	private Class<? extends Reducer<K1, V1, K2, V2>> reduceClass;
	private transient Reducer<K1, V1, K2, V2> reduceObj;
	private transient Reporter reporter;
	private OutputCollector<K2, V2> outputCollector;
	private String configFile;

	public Class<? extends Reducer<K1, V1, K2, V2>> getReduceClass() {
		return reduceClass;
	}

	public void setReduceClass(Class<? extends Reducer<K1, V1, K2, V2>> reduceClass) {
		this.reduceClass = reduceClass;
	}

	public String getConfigFile() {
		return configFile;
	}

	public void setConfigFile(String configFile) {
		this.configFile = configFile;
	}

	private int numberOfMappersRunning = -1;
	private int operatorId;

	public final transient DefaultInputPort<KeyHashValPair<Integer, Integer>> inputCount = new DefaultInputPort<KeyHashValPair<Integer, Integer>>() {
		@Override
		public void process(KeyHashValPair<Integer, Integer> tuple) {
			logger.info("processing {}", tuple);
			if (numberOfMappersRunning == -1)
				numberOfMappersRunning = tuple.getValue();
			else
				numberOfMappersRunning += tuple.getValue();

		}

	};

	public final transient DefaultOutputPort<KeyHashValPair<K2, V2>> output = new DefaultOutputPort<KeyHashValPair<K2, V2>>();
	private Map<K1, List<V1>> cacheObject;
	public final transient DefaultInputPort<KeyHashValPair<K1, V1>> input = new DefaultInputPort<KeyHashValPair<K1, V1>>() {

		@Override
		public void process(KeyHashValPair<K1, V1> tuple) {
			// logger.info("processing tupple {}",tuple);
			List<V1> list = cacheObject.get(tuple.getKey());
			if (list == null) {
				list = new ArrayList<V1>();
				list.add(tuple.getValue());
				cacheObject.put(tuple.getKey(), list);
			} else {
				list.add(tuple.getValue());
			}
		}

	};

	@Override
	public void setup(OperatorContext context) {
		reporter = new ReporterImpl(ReporterType.Reducer, new Counters());
		if(context != null)
			operatorId = context.getId();
		cacheObject = new HashMap<K1, List<V1>>();
		outputCollector = new OutputCollectorImpl<K2, V2>();
		if (reduceClass != null) {
			try {
				reduceObj = reduceClass.newInstance();
			} catch (Exception e) {
				logger.info("can't instantiate object {}", e.getMessage());
			}
			Configuration conf = new Configuration();
			InputStream stream = null;
			if (configFile != null && configFile.length() > 0) {
				logger.info("system /{}", configFile);
				stream = ClassLoader.getSystemResourceAsStream("/" + configFile);

				if (stream == null) {
					logger.info("system {}", configFile);
					stream = ClassLoader.getSystemResourceAsStream(configFile);

				}
			}

			if (stream != null) {
				logger.info("found our stream... so adding it");
				conf.addResource(stream);
			}
			reduceObj.configure(new JobConf(conf));
		}

	}

	@Override
	public void teardown() {

	}

	@Override
	public void beginWindow(long windowId) {

	}

	@Override
	public void endWindow() {
		if (numberOfMappersRunning == 0) {
			for (Map.Entry<K1, List<V1>> e : cacheObject.entrySet()) {
				try {
					reduceObj.reduce(e.getKey(), e.getValue().iterator(), outputCollector, reporter);
				} catch (IOException e1) {
					logger.info(e1.getMessage());
				}
			}
			List<KeyHashValPair<K2, V2>> list = ((OutputCollectorImpl<K2, V2>) outputCollector).getList();

			for (KeyHashValPair<K2, V2> e : list) {
				// logger.info("emitting tupple {}",e);
				output.emit(e);
				// logger.info("done emitting tupple {}",e);
			}
			list.clear();
			cacheObject.clear();
			numberOfMappersRunning = -1;
		}

	}

}
