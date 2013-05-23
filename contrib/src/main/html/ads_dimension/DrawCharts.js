/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
    

/**
 * Darw Charts function.
 * @author Dinesh Prasad(dinesh@malhar-inc.com)
 */
function DrawCharts(data)
{
  var dataArr = JSON.parse(data);
  DrawCostChart(dataArr);
  //document.getElementById("chart_div").innerHTML = data;
  DrawRevenueChart(dataArr);
  DrawClickChart(dataArr);
  DrawImpressionChart(dataArr);
  DrawMarginChart(dataArr);
  DrawCtrChart(dataArr);
}

/**
 * Fetch data, draw charts using new data, schedule future call after refresh interval.
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
function LoadCharts(query, shiftinterval) 
{
  // get query parameters 
  var params = SplitQuery(query);
		
  // fetch data, draw charts
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        DrawCharts(data);
      }
    }
    var url = "http://localhost/json.php?bucket=m";
    if (params['publisher']) 
    {	
      url += "&publisher=" + params['publisher'];
    }
    if (params['advertiser']) 
    {	
      url += "&advertiser=" + params['advertiser'];
    }
    if (params['adunit']) 
    {	
      url += "&adunit=" + params['adunit'];
    }
    url += "&from=";
    var from = (new Date().getTime() / 1000)-3600;
    if (params['chartwindow'] == "day") 
    {
      from = (new Date().getTime() / 1000)-(3600*24);
    }
    var from = (new Date().getTime() / 1000)-3600;
    if (params['chartwindow'] == "week") 
    {
      from = (new Date().getTime() / 1000)-(3600*24*7);
    }
    url += Math.floor(from);
    from += shiftinterval;
    connect.open('GET',  url, true);
    connect.send(null);
  } catch(e) {
  }
  shiftinterval += parseInt(params['refreshinterval']);
  var refresh = parseInt(params['refreshinterval']) * 1000;
  setInterval(function(){LoadCharts(query, shiftinterval);},refresh);
}

