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
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Machine Generated Data Example </title>

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

  // fetch data, draw charts
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {

console.log(url);
        aggrData = connect.response;
        var pts = JSON.parse(aggrData);
        aggrDataPoints = new Array();
        for(var i=0; i <  pts.length; i++) aggrDataPoints.push(pts[i]);
        DrawCPUChart();
        DrawRAMChart();
        DrawHDDChart();
        //DrawImpressionsChart();
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
  //document.getElementById('chart_div').innerHTML = url;

  // fetch data, draw charts
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        contData = connect.response;   
        var newPts = JSON.parse(contData); 
        contDataPoints = new Array();
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
  if (params['customer']) document.getElementById('customer').value = params['customer'];
  if (params['product']) document.getElementById('product').value = params['product'];
  if (params['os']) document.getElementById('os').value = params['os'];
  if (params['software1']) document.getElementById('software1').value = params['software1'];
  if (params['software2']) document.getElementById('software2').value = params['software2'];
  if (params['software3']) document.getElementById('software3').value = params['software3'];
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
  //DrawContCharts();
  setInterval(DrawAggrCharts, 30000);
  //setInterval(DrawContCharts, contRefresh * 1000);
};

</script>

</head>
<body>

    <div id="header">
        <ul class="dashboard-modes">
            <li>
                <a href="#" class="active">Machine Generated Data Example </a>
            </li>
        </ul>

    </div>
	
	<div id="main">
    <div id="pagecontent">
        <div class="dashboardMgr">
            <div class="inner" style="">
                <h2 class="title">View Real Time Data Charts</h2> 
                <form method="GET" action="index.php">
                    
                    <label for="customer">Customer ID:</label>
                    <select name="customer" id="customer" style="width:200px;">
                  		<option value="">ALL</option>
                		<?php
                   			for ($i = 1; $i <= 5; $i++) {
                  				print "<option value=\"$i\">Customer $i</option>\n";
                			}
                		?>
             		</select>
             		
            		<label for="">Product ID:</label>
            		<select name="product" id="product" style="width:200px;">
              		    <option value="">ALL</option>
                		<?php
                			for ($i = 4; $i <= 6; $i++) {
                  				print "<option value=\"$i\">Product $i</option>\n";
                			}
                		?>
            		</select>
        		
        		    <label for="">Product OS:</label>
            		<select name="os" id="os" style="width:200px;">
              		    <option value="">ALL</option>
        		        <?php
                			for ($i = 10; $i <= 12; $i++) {
                  				print "<option value=\"$i\">OS $i</option>\n";
                			}
        	            ?>
            		</select>
            		
                    <label for="software1">Software1 Ver:</label>
                    <select name="software1" id="software1" style="width:200px;">
                  		<option value="">ALL</option>
                		<?php
                   			for ($i = 10; $i <= 12; $i++) {
                  				print "<option value=\"$i\">Software1 Version $i</option>\n";
                			}
                		?>
             		</select>

                    <label for="software2">Software2 Ver:</label>
                    <select name="software2" id="software2" style="width:200px;">
                  		<option value="">ALL</option>
                		<?php
                   			for ($i = 12; $i <= 14; $i++) {
                  				print "<option value=\"$i\">Software2 Version $i</option>\n";
                			}
                		?>
             		</select>

                    <label for="software3">Software3 Ver:</label>
                    <select name="software3" id="software3" style="width:200px;">
                  		<option value="">ALL</option>
                		<?php
                   			for ($i = 4; $i <= 6; $i++) {
                  				print "<option value=\"$i\">Software3 Version $i</option>\n";
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
<!--        <div class="chart-ctnr" id="chart3_div" ></div>
        <div class="chart-ctnr" id="chart4_div" ></div>	
        <div class="chart-ctnr" id="chart5_div" ></div> -->
        </div>		
</body>
</html>
