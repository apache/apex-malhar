/*
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
/**
 * Declaration and initialization for global variables.
 */

// url parameters   
var params;

// Page View/Time
var pageViewData; 
var pageDataPoints; 
var pageViewTable;
var pageViewChart; 
var PageViewView;
var pageViewRefresh;
var pageViewLookback;
var pageViewUrl;
var pageViewInterval;
var pageNowPlaying;

// top url(s)
var topUrlTable;
var topUrlTableChart;

// server load  
var serverLoadRefresh;
var serverLoadLookback;
var serverName;
var serverLoadDataPoints;
var serverLoadTable;
var serverLoadChart;
var serverLoadView;
var serverLoadInterval;

// Top server(s)
var topServerTable;
var topServerTableChart;

var topIpClientTable;
var topIpClientTableChart;
var riskyClientTableChart;
var url404TableChart;
var server404TableChart;
var serverNowPlaying;


// Get split query string
function QueryString() {
  var query_string = {};
  var query = window.location.search.substring(1);
  return query;
}
function SplitQuery(query)
{  
	var params = {};
	var vars = query.split("&");
	for (var i=0;i<vars.length;i++)
	{
		var pair = vars[i].split("=");
		if(pair.length == 2) 
		{
			params[pair[0]] = pair[1];
		}
	}
	return params;
}  

// Initialize global variable(s)
function InitializeGlobal()
{
  // Initialize params  
  params = SplitQuery(QueryString()); 

  // intialize page view variables
  pageDataPoints = new Array();
  pageViewTable = new google.visualization.DataTable();
  pageViewTable.addColumn('datetime', 'Time');
  pageViewTable.addColumn('number', 'Page View');
  pageViewChart = new google.visualization.LineChart(document.getElementById('pageview_chart_div'));
  PageViewView = new google.visualization.DataView(pageViewTable);
  pageViewRefresh = 60;
  pageViewLookback = (new Date().getTime()/1000) - 3600;
  document.getElementById('pageviewlookback').value = "1";
  pageViewInterval = 1;

  serverLoadRefresh = 60;
  serverLoadLookback = (new Date().getTime()/1000) - 3600;
  document.getElementById('serverloadlookback').value = "1";
  serverLoadDataPoints = new Array();
  serverLoadTable = new google.visualization.DataTable();
  serverLoadTable.addColumn('datetime', 'Time');
  serverLoadTable.addColumn('number', 'Server Load');
  serverLoadChart = new google.visualization.LineChart(document.getElementById('server_load_div'));
  serverLoadView = new google.visualization.DataView(serverLoadTable);
  serverLoadInterval = 1;

  topUrlTableChart = new google.visualization.Table(document.getElementById('top_url_div'));
  topServerTableChart = new google.visualization.Table(document.getElementById('top_server_div'));

  topIpClientTableChart = new google.visualization.Table(document.getElementById('top_IpClient_div'));
  riskyClientTableChart = new google.visualization.Table(document.getElementById('top_ipdata_div'));

  url404TableChart = new google.visualization.Table(document.getElementById('url_404_div'));
  server404TableChart = new google.visualization.Table(document.getElementById('server_404_div'));
}

/**
 * Sort json array  
 */
function sortByKey(array, key) {
    return array.sort(function(a, b) {
        var x = a[key]; var y = b[key];
        return ((x < y) ? -1 : ((x > y) ? 1 : 0));
    });
}

