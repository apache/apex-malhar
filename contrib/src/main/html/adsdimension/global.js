/*
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

// Cost data table 
var costTable;
var costChart; 
var costView;

// Revenue data table 
var revenueTable;
var revenueChart; 
var revenueView;  

// Clicks data table 
var clicksTable;
var clicksChart; 
var clicksView;  

// Impressions data table 
var impressionsTable;
var impressionsChart; 
var impressionsView;  

// Ctr data table 
var ctrTable;
var ctrChart; 
var ctrView;  

// Margin data table 
var marginTable;
var marginChart; 
var marginView;  

// chart options
var chartOptions;

// Date formatter  
var dateFormatter;

// window look back value 
var lookback;
var aggrLookBack;
var contLookBack;
var contRefresh;
var paramLookBack;

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
    
  // Initialize cost table 
  costTable = new google.visualization.DataTable(); 
  costTable.addColumn('datetime', 'Time');
  costTable.addColumn('number', 'Cost');
  chartOptions = { width: 600, height: 300, legend: 'none', pointSize: 0, lineWidth : 1 };
  costChart = new google.visualization.ScatterChart(document.getElementById('chart_div'));
  costView = new google.visualization.DataView(costTable);

  // Initialize revenue table 
  revenueTable = new google.visualization.DataTable(); 
  revenueTable.addColumn('datetime', 'Time');
  revenueTable.addColumn('number', 'Revenue');;
  revenueChart = new google.visualization.ScatterChart(document.getElementById('chart1_div'));
  revenueView = new google.visualization.DataView(revenueTable);

  // Initialize clicks table 
  clicksTable = new google.visualization.DataTable(); 
  clicksTable.addColumn('datetime', 'Time');
  clicksTable.addColumn('number', 'Clicks');;
  clicksChart = new google.visualization.ScatterChart(document.getElementById('chart2_div'));
  clicksView = new google.visualization.DataView(clicksTable);
    
  // Initialize impressions table 
  impressionsTable = new google.visualization.DataTable(); 
  impressionsTable.addColumn('datetime', 'Time');
  impressionsTable.addColumn('number', 'Impressions');;
  impressionsChart = new google.visualization.ScatterChart(document.getElementById('chart3_div'));
  impressionsView = new google.visualization.DataView(impressionsTable);
    
  // Initialize ctr table 
  ctrTable = new google.visualization.DataTable(); 
  ctrTable.addColumn('datetime', 'Time');
  ctrTable.addColumn('number', 'Ctr');;
  ctrChart = new google.visualization.ScatterChart(document.getElementById('chart4_div'));
  ctrView = new google.visualization.DataView(ctrTable);
    
  // Initialize margin table 
  marginTable = new google.visualization.DataTable(); 
  marginTable.addColumn('datetime', 'Time');
  marginTable.addColumn('number', 'Margin');;
  marginChart = new google.visualization.ScatterChart(document.getElementById('chart5_div'));
  marginView = new google.visualization.DataView(marginTable);

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
    url += "&publisher=";
    if (params['publisher']) 
    {	
      url += params['publisher'];
    }
    url += "&advertiser=";
    if (params['advertiser']) 
    {	
      url += params['advertiser'];
    }
    url += "&adunit=";
    if (params['adunit']) 
    {	
      url += params['adunit'];
    }
     url += "&from=";
    url += Math.floor(lookback);
    return url;   
}

/**
 * Creates data table with time stamp and cost values.
 * Draw line chart for time vs cost.
 */
function DrawCostChart()
{
  // create/delete rows 
  if (costTable.getNumberOfRows() < aggrDataPoints.length)
  {    
    var numRows = aggrDataPoints.length - costTable.getNumberOfRows();
    costTable.addRows(numRows);
  } else {
    for(var i=(costTable.getNumberOfRows()-1); i >= aggrDataPoints.length; i--)
    {
      costTable.removeRow(i);    
    }
  }
    
  // Populate data table with time/cost data points. 
  for(var i=0; i < costTable.getNumberOfRows(); i++)
  {
    //if(parseFloat(aggrDataPoints[i].cost) < 500) continue;
    costTable.setCell(i, 0, new Date(parseInt(aggrDataPoints[i].timestamp)));
    costTable.setCell(i, 1, parseFloat(aggrDataPoints[i].cost));
  }

  // Draw line chart.
  chartOptions.title = 'Cost Chart';
  costChart.draw(costView, chartOptions); 
}     

/**
 * Creates data table with time stamp and revenu values.
 * Draw line chart for time vs revenue.
 */
function DrawRevenueChart()
{
  // create/delete rows 
  if (revenueTable.getNumberOfRows() < aggrDataPoints.length)
  {    
    var numRows = aggrDataPoints.length - revenueTable.getNumberOfRows();
    revenueTable.addRows(numRows);
  } else {
    for(var i=(revenueTable.getNumberOfRows()-1); i >= aggrDataPoints.length; i--)
    {
      revenueTable.removeRow(i);    
    }
  }

  // Populate data table with time/revenue data points. 
  for(var i=0; i < revenueTable.getNumberOfRows(); i++)
  {
    revenueTable.setCell(i, 0, new Date(parseInt(aggrDataPoints[i].timestamp)));
    revenueTable.setCell(i, 1, parseFloat(aggrDataPoints[i].revenue));
  }

  // Draw line chart.
  chartOptions.title = 'Revenue Chart';
  revenueChart.draw(revenueView, chartOptions); 
}  

/**
 * Creates data table with time stamp and clicks values.
 * Draw line chart for time vs clicks.
 */
function DrawClicksChart()
{
  // create/delete rows 
  if (clicksTable.getNumberOfRows() < aggrDataPoints.length)
  {    
    var numRows = aggrDataPoints.length - clicksTable.getNumberOfRows();
    clicksTable.addRows(numRows);
  } else {
    for(var i=(clicksTable.getNumberOfRows()-1); i >= aggrDataPoints.length; i--)
    {
      clicksTable.removeRow(i);    
    }
  }

  // Populate data table with time/clicks data points. 
  for(var i=0; i < clicksTable.getNumberOfRows(); i++)
  {
    clicksTable.setCell(i, 0, new Date(parseInt(aggrDataPoints[i].timestamp)));
    clicksTable.setCell(i, 1, parseInt(aggrDataPoints[i].clicks));
  }

  // Draw line chart.
  chartOptions.title = 'Clicks Chart';
  clicksChart.draw(clicksView, chartOptions); 
}

/**
 * Creates data table with time stamp and impressions values.
 * Draw line chart for time vs impressions.
 */
function DrawImpressionsChart()
{
  // create/delete rows 
  if (impressionsTable.getNumberOfRows() < aggrDataPoints.length)
  {    
    var numRows = aggrDataPoints.length - impressionsTable.getNumberOfRows();
    impressionsTable.addRows(numRows);
  } else {
    for(var i=(impressionsTable.getNumberOfRows()-1); i >= aggrDataPoints.length; i--)
    {
      impressionsTable.removeRow(i);    
    }
  }

  // Populate data table with time/impressions data points. 
  for(var i=0; i < impressionsTable.getNumberOfRows(); i++)
  {
    impressionsTable.setCell(i, 0, new Date(parseInt(aggrDataPoints[i].timestamp)));
    impressionsTable.setCell(i, 1, parseInt(aggrDataPoints[i].impressions));
  }

  // Draw line chart.
  chartOptions.title = 'Impressions Chart';
  impressionsChart.draw(impressionsView, chartOptions); 
}

/**
 * Draw line chart for time vs ctr.
 */
function DrawCtrChart()
{
  // create/delete rows 
  if (ctrTable.getNumberOfRows() < contDataPoints.length)
  {    
    var numRows = contDataPoints.length - ctrTable.getNumberOfRows();
    ctrTable.addRows(numRows);
  } else {
    for(var i=(ctrTable.getNumberOfRows()-1); i > contDataPoints.length; i--)
    {
      ctrTable.removeRow(i);    
    }
  }

  // Populate data table with time/cost data points. 
  for(var i=0; i < ctrTable.getNumberOfRows(); i++)
  {
    ctrTable.setCell(i, 0, new Date(parseInt(contDataPoints[i].timestamp)));
    ctrTable.setCell(i, 1, (parseInt(contDataPoints[i].clicks)/parseInt(contDataPoints[i].impressions))*100);
  }

  // Draw line chart.
  chartOptions.title = 'Ctr Chart';
  ctrChart.draw(ctrView, chartOptions); 
} 

/**
 * Draw line chart for time vs margin.
 */
function DrawMarginChart()
{
  // create/delete rows 
  if (marginTable.getNumberOfRows() < contDataPoints.length)
  {    
    var numRows = contDataPoints.length - marginTable.getNumberOfRows();
    marginTable.addRows(numRows);
  } else {
    for(var i=(marginTable.getNumberOfRows()-1); i > contDataPoints.length; i--)
    {
      marginTable.removeRow(i);    
    }
  }

  // Populate data table with time/cost data points. 
  for(var i=0; i < marginTable.getNumberOfRows(); i++)
  {
    marginTable.setCell(i, 0, new Date(parseInt(contDataPoints[i].timestamp)));
    marginTable.setCell(i, 1, (parseFloat(contDataPoints[i].cost)-parseFloat(contDataPoints[i].revenue))/parseFloat(contDataPoints[i].revenue));
  }

  // Draw line chart.
  chartOptions.title = 'Margin Chart';
  marginChart.draw(marginView, chartOptions); 
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

