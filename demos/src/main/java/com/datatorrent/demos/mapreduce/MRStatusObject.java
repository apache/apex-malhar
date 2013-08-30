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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.jettison.json.JSONObject;

/**
 * <p>MRStatusObject class.</p>
 *
 * @since 0.3.4
 */
public class MRStatusObject {
	private String uri;
	private String jobId;
	private String apiVersion;
	private int hadoopVersion;
	private String appId;
	private int rmPort;
	private int historyServerPort;
	private JSONObject jsonObject;
	private Map<String, JSONObject> mapJsonObject;
	private Map<String, JSONObject> reduceJsonObject;

	public MRStatusObject(){
		mapJsonObject = new ConcurrentHashMap<String, JSONObject>();
		reduceJsonObject = new ConcurrentHashMap<String, JSONObject>();
	}


	public Map<String, JSONObject> getMapJsonObject() {
		return mapJsonObject;
	}
	public void setMapJsonObject(Map<String, JSONObject> mapJsonObject) {
		this.mapJsonObject = mapJsonObject;
	}
	public Map<String, JSONObject> getReduceJsonObject() {
		return reduceJsonObject;
	}
	public void setReduceJsonObject(Map<String, JSONObject> reduceJsonObject) {
		this.reduceJsonObject = reduceJsonObject;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getJobId() {
		return jobId;
	}
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	public String getApiVersion() {
		return apiVersion;
	}
	public void setApiVersion(String apiVersion) {
		this.apiVersion = apiVersion;
	}
	public int getHadoopVersion() {
		return hadoopVersion;
	}
	public void setHadoopVersion(int hadoopVersion) {
		this.hadoopVersion = hadoopVersion;
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public int getRmPort() {
		return rmPort;
	}
	public void setRmPort(int rmPort) {
		this.rmPort = rmPort;
	}
	public int getHistoryServerPort() {
		return historyServerPort;
	}
	public void setHistoryServerPort(int historyServerPort) {
		this.historyServerPort = historyServerPort;
	}
	public JSONObject getJsonObject() {
		return jsonObject;
	}
	public void setJsonObject(JSONObject jsonObject) {
		this.jsonObject = jsonObject;
	}



}
