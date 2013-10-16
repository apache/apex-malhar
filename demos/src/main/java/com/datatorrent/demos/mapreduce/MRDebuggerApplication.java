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
package com.datatorrent.demos.mapreduce;

import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.PubSubWebSocketInputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;

/**
 * <p>MRDebuggerApplication class.</p>
 *
 * @since 0.3.4
 */
public class MRDebuggerApplication implements StreamingApplication {

	private static final Logger LOG = LoggerFactory
			.getLogger(MRDebuggerApplication.class);

	@Override
	public void populateDAG(DAG dag, Configuration arg1) {
		String gatewayAddress = dag.attrValue(DAG.GATEWAY_ADDRESS, null);
		if (gatewayAddress== null || StringUtils.isEmpty(gatewayAddress)) {
			gatewayAddress = "10.0.2.15:9790";
		}
		MRJobStatusOperator mrJobOperator = dag.addOperator(
				"mrJobStatusOperator", new MRJobStatusOperator());


		URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
		LOG.info("WebSocket with gateway at: {}", gatewayAddress);

		PubSubWebSocketInputOperator wsIn = dag.addOperator("mrDebuggerQueryWS", new PubSubWebSocketInputOperator());
		wsIn.setUri(uri);
		wsIn.addTopic("contrib.summit.mrDebugger.mrDebuggerQuery");

		dag.addStream("query", wsIn.outputPort, mrJobOperator.input);

		PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator("mrDebuggerJobResultWS",new PubSubWebSocketOutputOperator<Object>());
		wsOut.setUri(uri);
		wsOut.setTopic("contrib.summit.mrDebugger.jobResult");

		PubSubWebSocketOutputOperator<Object> wsMapOut = dag.addOperator("mrDebuggerMapResultWS",new PubSubWebSocketOutputOperator<Object>());
		wsMapOut.setUri(uri);
		wsMapOut.setTopic("contrib.summit.mrDebugger.mapResult");

		PubSubWebSocketOutputOperator<Object> wsReduceOut = dag.addOperator("mrDebuggerReduceResultWS",new PubSubWebSocketOutputOperator<Object>());
		wsReduceOut.setUri(uri);
		wsReduceOut.setTopic("contrib.summit.mrDebugger.reduceResult");


		dag.addStream("jobConsoledata", mrJobOperator.output,wsOut.input);
		dag.addStream("mapConsoledata", mrJobOperator.mapOutput,wsMapOut.input);
		dag.addStream("reduceConsoledata", mrJobOperator.reduceOutput,wsReduceOut.input);


	}

}
