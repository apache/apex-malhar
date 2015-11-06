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
 * Functions for drawing page view vs time chart.
 */


function PageViewTimeDataUrl()
{    
    var url = "PageViewTimeData.php?";
    url += "from=";
    url += Math.floor(pageViewLookback);
    if (pageViewUrl) 
    {
       url += "&url=" + pageViewUrl;   
    }
    //url += "&url=mydomain.com/services.php?serviceid=6";
    return url;  
}

function RenderPageViewTimeChart()
{
  // create/delete rows 
  if (pageViewTable.getNumberOfRows() < pageDataPoints.length)
  {    
    var numRows = pageDataPoints.length - pageViewTable.getNumberOfRows();
    pageViewTable.addRows(numRows);
  } else {
    for(var i=(pageViewTable.getNumberOfRows()-1); i >= pageDataPoints.length; i--)
    {
      pageViewTable.removeRow(i);    
    }
  }

  // Populate data table with time/cost data points. 
  for(var i=0; i < pageViewTable.getNumberOfRows(); i++)
  {
    //if(parseFloat(aggrDataPoints[i].cost) < 500) continue;
    pageViewTable.setCell(i, 0, new Date(parseInt(pageDataPoints[i].timestamp)));
    pageViewTable.setCell(i, 1, parseFloat(pageDataPoints[i].view));
  }
    
  // get options
  var page = document.getElementById('page').value;
  var index = document.getElementById('index').value;
  var title = "ALL Urls (PVS/Min)";
  if (page == "home") title = "home.php (PVS/Min)";
  if (page == "contact") title = "contactus.php (PVS/Min)";
  if (page == "about") title = "about.php (PVS/Min)";
  if (page == "support") title = "support.php (PVS/Min)";
  if (page == "product") {
    title = "product.php-" + index + " (PVS/Min)";
  }
  if (page == "services") {
    title = "services.php-" + index + " (PVS/Min)";
  }
  if (page == "products") {
    title = "products.php-" + index + " (PVS/Min)";
  }

  var options = { pointSize: 0, lineWidth : 1, legend : {position : 'top'} };
  options.title = title;

  // Draw line chart.
  pageViewChart.draw(PageViewView, options); 
}

function DrawPageViewTimeChart()
{
  var url = PageViewTimeDataUrl();
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        pageViewData = connect.response;
        var pts = JSON.parse(pageViewData);
        for(var i=0; i <  pts.length; i++) 
        {
          pageDataPoints.push(pts[i]);
          delete pts[i];
        }
        delete pts;
        sortByKey(pageDataPoints, "timestamp");
        RenderPageViewTimeChart();
        delete pageViewData;
        delete pageDataPoints;
        pageDataPoints = new Array();
      }
    }
    connect.open('GET',  url, true);
    connect.send(null);
  } catch(e) {
  }
  pageViewLookback = (new Date().getTime()/1000) - (3600 * pageViewInterval)-60;
}


function HandlePageViewTimeSubmit()
{
  // remove old time  
  if(pageNowPlaying) clearInterval(pageNowPlaying); 

  // get submit values 
  var page = document.getElementById('page').value;
  var index = document.getElementById('index').value;
  if (page == "all") pageViewUrl ="";
  if (page == "home") pageViewUrl = "mydomain.com/home.php";
  if (page == "contact") pageViewUrl = "mydomain.com/contactus.php";
  if (page == "about") pageViewUrl = "mydomain.com/about.php";
  if (page == "support") pageViewUrl = "mydomain.com/support.php";
  if (page == "product")
  {
    pageViewUrl = "mydomain.com/products.php";   
    if (index && (index.length > 0)) pageViewUrl += "?productid=" + index;
  }
  if (page == "services") 
  {
    pageViewUrl = "mydomain.com/services.php";   
    if (index && (index.length > 0)) pageViewUrl += "?serviceid=" + index;
  }
  if (page == "partners") 
  {
    pageViewUrl = "mydomain.com/partners.php";   
    if (index && (index.length > 0)) pageViewUrl += "?partnerid=" + index;
  }
  pageViewLookback = document.getElementById('pageviewlookback').value;
  if ( !pageViewLookback || (pageViewLookback == "")) {
    pageViewLookback = (new Date().getTime()/1000) - 3600;
  }  else {
    pageViewLookback = (new Date().getTime()/1000) - 3600 * pageViewLookback;
  }

  // set from values  
  document.getElementById('page').value = page;
  document.getElementById('index').value = index;
  var lookback = document.getElementById('pageviewlookback').value;
  document.getElementById('pageviewlookback').value = lookback;
  pageViewInterval = lookback;
    
  // draw chart
  DrawPageViewTimeChart();
  pageNowPlaying = setInterval(DrawPageViewTimeChart, 60 * 1000);
}

function handleUrlChange()
{
  var page = document.getElementById('page').value;
  if ((page == "home")||(page == "contact")||(page == "about")||(page == "support") || (page =="all"))
  {
    document.getElementById('index').value = 0;
    document.getElementById('index').disabled = "true";   
  } else {
    document.getElementById('index').value = 0;
    document.getElementById('index').disabled = ""; 
  }
}
