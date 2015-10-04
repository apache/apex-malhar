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
<!--
 --  Copyright (c) 2012-2013 DataTorrent, Inc.
 --  All Rights Reserved.
 -->
    
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Data Torrent : Ads Demo </title>

<link rel="stylesheet" type="text/css" href="malhar.css">

<!-- Google charts include -->
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script type="text/javascript">
google.load('visualization', '1', {'packages':['corechart']});
</script>

<!-- Malhar charting utils -->
<script type="text/javascript" src="global.js"></script>

<!-- window onload -->
<script type="text/javascript">

function DrawAggrCharts()
{
  // get refresh url 
  lookback = aggrLookBack; 
  var url = DataUrl();  
  //document.getElementById('chart_div').innerHTML = url;

  // fetch data, draw charts
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        aggrData = connect.response;
        var pts = JSON.parse(aggrData);
        aggrDataPoints  = new Array();
        for(var i=0; i <  pts.length; i++) aggrDataPoints.push(pts[i]);
        DrawCostChart();
        DrawRevenueChart();
        DrawClicksChart();
        DrawImpressionsChart();
        delete aggrData;
      }
    }
    connect.open('GET',  url, true);
    connect.send(null);
  } catch(e) {
  }
  aggrLookBack += 30;
}

function DrawContCharts()  
{    
  // get refresh url 
  lookback = contLookBack; 
  var url = DataUrl();    

  // fetch data, draw charts
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        contData = connect.response;   
        var newPts = JSON.parse(contData);  
        contDataPoints  = new Array();
        for(var i=0; i <  newPts.length; i++) contDataPoints.push(newPts[i]);
        DrawCtrChart() ;
        DrawMarginChart();
        delete contData;
        delete newPts;
      }
    }
    connect.open('GET',  url, true);
    connect.send(null);
  } catch(e) {
  }
  contLookBack += contRefresh;
}

window.onload = function() {

  // Initialize global 
  InitializeGlobal();   

  // Inituialize form fields  
  if (params['publisher']) document.getElementById('publisher').value = params['publisher'];
  if (params['advertiser']) document.getElementById('advertiser').value = params['advertiser'];
  if (params['adunit']) document.getElementById('adunit').value = params['adunit'];
  if (params['refresh'])
  {
    document.getElementById('refresh').value = params['refresh'];   
  } else {
    document.getElementById('refresh').value = 5;
  }    
  if (params['lookback'])
  {
    document.getElementById('lookback').value = params['lookback'];   
  } else {
    document.getElementById('lookback').value = 6;
  }
       

  // draw charts 
  DrawAggrCharts();
  DrawContCharts();
  setInterval(DrawAggrCharts, 30000);
  setInterval(DrawContCharts, contRefresh * 1000);
};

</script>

</head>
<body>

    <div id="header">
        <ul class="dashboard-modes">
            <li>
                <a href="#" class="active">Ads Dimensions Demo</a>
            </li>
        </ul>

        <div id="logo"><img src="main_banner.png"/></div>
    </div>
	
	<div id="main">
    <div id="pagecontent">
        <div class="dashboardMgr">
            <div class="inner" style="">
                <h2 class="title">View Real Time Data Charts</h2> 
                <form method="GET" action="index.php">
                    
                    <label for="publisher">Publisher ID:</label>
                    <select name="publisher" id="publisher" style="width:200px;">
                  		<option value="">ALL</option>
                		<?php
                   			for ($i = 0; $i < 50; $i++) {
                  				print "<option value=\"$i\">Publisher $i</option>\n";
                			}
                		?>
             		</select>
             		
            		<label for="">Advertiser ID:</label>
            		<select name="advertiser" id="advertiser" style="width:200px;">
              		    <option value="">ALL</option>
                		<?php
                			for ($i = 0; $i < 100; $i++) {
                  				print "<option value=\"$i\">Advertiser $i</option>\n";
                			}
                		?>
            		</select>
        		
        		    <label for="">Ad Unit:</label>
            		<select name="adunit" id="adunit" style="width:200px;">
              		    <option value="">ALL</option>
        		        <?php
                			for ($i = 0; $i < 5; $i++) {
                  				print "<option value=\"$i\">Adunit $i</option>\n";
                			}
        	            ?>
            		</select>
            		
            		<label for="">Refresh Interval:</label>
            		<div class="input-append">
                        <input type="text" name="refresh" id="refresh" class="input-small"/>
                        <span class="add-on">Secs</span>
                    </div>
                    

        		    <label for="">Look Back:</label>
        		    <div class="input-append">
                        <input type="text" name="lookback" id="lookback" class="input-small"/>
                        <span class="add-on">Hours</span>
                    </div>
                    
                    <input type="submit" value="submit" class="btn btn-primary" />
                    
                </form>
            </div>
            <div class="collapser-container">
                <div class="collapser">
                    <div class="collapse-dot"></div>
                    <div class="collapse-dot"></div>
                    <div class="collapse-dot"></div>
                </div>
            </div>
        </div>
        <div class="dashboardMain">
            
	<!-- <table><tbody>
                <tr>
        	      <td><div id="chart_div"></div></td>	
        	      <td><div id="chart1_div" ></div></td>	
                 </tr>
                 <tr>
        	     <td><div id="chart2_div" ></div></td>	
        	     <td><div id="chart3_div" ></div></td>	
                 </tr>
                 <tr>
        	   <td><div id="chart4_div" ></div></td>	
        	    <td><div id="chart5_div" ></div></td>	
                 </tr>
        	 </tr></tbody></table> -->
	<div class="chart-ctnr" id="chart_div"></div>
        <div class="chart-ctnr" id="chart1_div" ></div>	
        <div class="chart-ctnr" id="chart2_div" ></div>	
        <div class="chart-ctnr" id="chart3_div" ></div>	
        <div class="chart-ctnr" id="chart4_div" ></div>	
        <div class="chart-ctnr" id="chart5_div" ></div>
        </div>		
</body>
</html>
