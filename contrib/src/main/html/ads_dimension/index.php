<!--
 --  Copyright (c) 2012-2013 Malhar, Inc.
 --  All Rights Reserved.
 -->
    
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Data Torrent : STREAMS COME TRUE! REAL-TIME HEDOOP </title>

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
        aggrData = connect.response;
        aggrDataPoints = JSON.parse(aggrData);
        DrawCostChart();
        DrawRevenueChart();
        DrawClicksChart();
        DrawImpressionsChart();
        delete aggrData;
        for(var i=0; i < aggrDataPoints.length; i++) delete aggrDataPoints[i];
        delete aggrDataPoints;
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
        contDataPoints = JSON.parse(contData);
        DrawCtrChart();
        DrawMarginChart();
        delete contData;
        for(var i=0; i < contDataPoints.length; i++) delete contDataPoints[i];
        delete contDataPoints;
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
  if (params['publisher'])
  {    
    document.getElementById('publisher').value = "";
    if (params['publisher'].length > 0) document.getElementById('publisher').value = "Publisher " + params['publisher'];
  }  
  if (params['advertiser'])
  {    
    document.getElementById('advertiser').value = "";
    if (params['advertiser'].length > 0) document.getElementById('advertiser').value = "Advertiser " + params['advertiser'];
  }
  if (params['adunit'])
  {    
    document.getElementById('adunit').value = "";
    if (params['adunit'].length > 0) document.getElementById('adunit').value = "Adunit " + params['adunit'];
  }
  if (params['refresh'] && (params['refresh'].length > 0))
  {
    document.getElementById('refresh').value = params['refresh'];   
  } else {
    document.getElementById('refresh').value = 5;
  }    
  if (params['lookback'] && (params['lookback'].length > 0))
  {
    document.getElementById('lookback').value = params['lookback'];   
  } else {
    document.getElementById('lookback').value = 6;
  } 
       
  // draw charts 
  DrawAggrCharts();
  DrawContCharts();
  setInterval(DrawAggrCharts, 30000);
  setInterval(DrawContCharts, contRefresh);
};

</script>

</head>
<body>

    <div id="header">
        <table><tbody>
			<td><div id="logo"><img src="main_banner.png"/></div></td>
			<td><div id="slogan">
				<h1>REAL TIME HADOOP, REALLY!</h1>
			</div></td>
    	</tbody></table>
    </div>
		
	<div id="maincontent">

        <h2>View Real Time Data Charts</h2> 
        <form method="GET" action="index.php">
        <table><tbody><tr>
           <td>
                Publisher ID: <br> 
                <select name="publisher">
  		<option value="">ALL</option>
		<?php
   			for ($i = 0; $i < 50; $i++) {
      				print "<option value=\"$i\">Publisher $i</option>\n";
    			}
		?>
 		</select>
           </td>
           <td>
		Advertiser ID: <br>
		<select name="advertiser">
  		<option value="">ALL</option>
		<?php
    			for ($i = 0; $i < 200; $i++) {
      				print "<option value=\"$i\">Advertiser $i</option>\n";
    			}
		?>
		</select>
           </td>
           <td>
		Ad Unit: <br>
    		<select name="adunit">
      		<option value="">ALL</option>
		<?php
    			for ($i = 0; $i < 5; $i++) {
      				print "<option value=\"$i\">Adunit $i</option>\n";
    			}
	        ?>
    		</select>
           </td>
           <td>
		Refresh Interval: <br>
                <input type="textfield" size="4" name="refresh" id="refresh" />Secs
           </td>
           <td>
           <td>
		Look Back: <br>
                <input type="textfield" size="4" name="lookback" id="lookback" />Hours
           </td>
        </tr>
        <tr><td><input type="submit" value="SUBMIT" />
        </tbody></table>
        </form>

	<table><tbody>
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
	 </tr></tbody></table>
	</div>		
</body>
</html>
