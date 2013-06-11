/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Declaration and initialization for global variables.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
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

// top ip 
var topIpClientTable;
var topIpClientTableChart;

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
  pageViewRefresh = 5;
  pageViewLookback = (new Date().getTime()/1000) - 3600;
  document.getElementById('pageviewrefresh').value = "5";
  document.getElementById('pageviewlookback').value = "1";
  pageViewInterval = 1;

  serverLoadRefresh = 5;
  serverLoadLookback = (new Date().getTime()/1000) - 3600;
  document.getElementById('serverloadrefresh').value = "5";
  document.getElementById('serverloadlookback').value = "1";
  serverLoadDataPoints = new Array();
  serverLoadTable = new google.visualization.DataTable(); 
  serverLoadTable.addColumn('datetime', 'Time');
  serverLoadTable.addColumn('number', 'Server Load');
  serverLoadChart = new google.visualization.LineChart(document.getElementById('server_load_div'));
  serverLoadView = new google.visualization.DataView(serverLoadTable);
  serverLoadInterval = 1;
 
  topUrlTableChart = new google.visualization.Table(document.getElementById('top_url_div'));
  topIpClientTableChart = new google.visualization.Table(document.getElementById('top_IpClient_div'));
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

