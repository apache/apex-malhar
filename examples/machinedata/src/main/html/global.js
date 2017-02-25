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

// Data Points 
var aggrData; 
var aggrDataPoints;
var contData;
var contDataPoints;

// CPU data table 
var cpuTable;
var cpuChart; 
var cpuView;

// ram data table 
var ramTable;
var ramChart; 
var ramView;  

// hdd data table 
var hddTable;
var hddChart; 
var hddView;  

// chart options
var chartOptions;

// Date formatter  
var dateFormatter;

// window look back value 
var lookback;
var aggrLookBack;
var contLookBack;
var contRefresh;

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
       
  // Initialize data points 
  aggrDataPoints = new Array();
  contDataPoints = new Array();
    
  // Initialize cpu table 
  cpuTable = new google.visualization.DataTable(); 
  cpuTable.addColumn('datetime', 'Time');
  cpuTable.addColumn('number', 'CPU');
  chartOptions = { width: 600, height: 300, legend: 'none', pointSize: 0, lineWidth : 1 };
  cpuChart = new google.visualization.ScatterChart(document.getElementById('chart_div'));
  cpuView = new google.visualization.DataView(cpuTable);

  // Initialize ram table 
  ramTable = new google.visualization.DataTable(); 
  ramTable.addColumn('datetime', 'Time');
  ramTable.addColumn('number', 'RAM');;
  ramChart = new google.visualization.ScatterChart(document.getElementById('chart1_div'));
  ramView = new google.visualization.DataView(ramTable);

  // Initialize hdd table 
  hddTable = new google.visualization.DataTable(); 
  hddTable.addColumn('datetime', 'Time');
  hddTable.addColumn('number', 'HDD');;
  hddChart = new google.visualization.ScatterChart(document.getElementById('chart2_div'));
  hddView = new google.visualization.DataView(hddTable);
    
  // get lookback value  
  lookback = (new Date().getTime()/1000) - 3600*6;
  if (params['lookback'] && (params['lookback'].length > 0)) lookback = (new Date().getTime()/1000) - (3600*(parseInt(params['lookback'])));
  aggrLookBack = lookback;
     
  // get continuos lookback 
  contLookBack = lookback;
  contRefresh = 5;

  // get param lookback  
  paramLookBack = 6;
  if (params['lookback'] && (params['lookback'].length > 0)) paramLookBack = parseInt(params['lookback']);
  //if (params['refresh'] && (params['refresh'].length > 0)) contRefresh = parseInt(params['refresh']);
}


/**
 * Function to create fetch urls from given parameters
 */
function DataUrl() 
{       
    var url = "json.php?bucket=m";
    url += "&customer=";
    if (params['customer'])
    {	
      url += params['customer'];
    }
    url += "&product=";
    if (params['product'])
    {	
      url += params['product'];
    }
    url += "&os=";
    if (params['os'])
    {	
      url += params['os'];
    }
    url += "&software1=";
    if (params['software1'])
    {
      url += params['software1'];
    }
    url += "&software2=";
    if (params['software2'])
    {
      url += params['software2'];
    }
    url += "&software3=";
    if (params['software3'])
    {
      url += params['software3'];
    }
     url += "&from=";
    url += Math.floor(lookback);
    return url;   
}

/**
 * Creates data table with time stamp and cpu values.
 * Draw line chart for time vs cpu.
 */
function DrawCPUChart()
{
  // create/delete rows 
  if (cpuTable.getNumberOfRows() < aggrDataPoints.length)
  {    
    var numRows = aggrDataPoints.length - cpuTable.getNumberOfRows();
    cpuTable.addRows(numRows);
  } else {
    for(var i=(cpuTable.getNumberOfRows()-1); i >= aggrDataPoints.length; i--)
    {
      cpuTable.removeRow(i);    
    }
  }
    
  // Populate data table with time/cpu data points. 
  for(var i=0; i < cpuTable.getNumberOfRows(); i++)
  {
    //if(parseFloat(aggrDataPoints[i].cpu) < 500) continue;
    cpuTable.setCell(i, 0, new Date(parseInt(aggrDataPoints[i].timestamp)));
    cpuTable.setCell(i, 1, parseFloat(aggrDataPoints[i].cpu));
  }

  // Draw line chart.
  chartOptions.title = 'CPU Usage (%)';
  cpuChart.draw(cpuView, chartOptions); 
}     

/**
 * Creates data table with time stamp and revenu values.
 * Draw line chart for time vs ram.
 */
function DrawRAMChart()
{
  // create/delete rows 
  if (ramTable.getNumberOfRows() < aggrDataPoints.length)
  {    
    var numRows = aggrDataPoints.length - ramTable.getNumberOfRows();
    ramTable.addRows(numRows);
  } else {
    for(var i=(ramTable.getNumberOfRows()-1); i >= aggrDataPoints.length; i--)
    {
      ramTable.removeRow(i);    
    }
  }

  // Populate data table with time/ram data points. 
  for(var i=0; i < ramTable.getNumberOfRows(); i++)
  {
    ramTable.setCell(i, 0, new Date(parseInt(aggrDataPoints[i].timestamp)));
    ramTable.setCell(i, 1, parseFloat(aggrDataPoints[i].ram));
  }

  // Draw line chart.
  chartOptions.title = 'RAM Usage (%)';
  ramChart.draw(ramView, chartOptions); 
}  

/**
 * Creates data table with time stamp and hdd values.
 * Draw line chart for time vs hdd.
 */
function DrawHDDChart()
{
  // create/delete rows 
  if (hddTable.getNumberOfRows() < aggrDataPoints.length)
  {    
    var numRows = aggrDataPoints.length - hddTable.getNumberOfRows();
    hddTable.addRows(numRows);
  } else {
    for(var i=(hddTable.getNumberOfRows()-1); i >= aggrDataPoints.length; i--)
    {
      hddTable.removeRow(i);    
    }
  }

  // Populate data table with time/hdd data points. 
  for(var i=0; i < hddTable.getNumberOfRows(); i++)
  {
    hddTable.setCell(i, 0, new Date(parseInt(aggrDataPoints[i].timestamp)));
    hddTable.setCell(i, 1, parseInt(aggrDataPoints[i].hdd));
  }

  // Draw line chart.
  chartOptions.title = 'HDD Usage (%)';
  hddChart.draw(hddView, chartOptions); 
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

