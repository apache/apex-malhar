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
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script type="text/javascript" src="util.js"></script>
<script type="text/javascript" src="DrawCost.js"></script>
<script type="text/javascript" src="DrawRevenue.js"></script>
<script type="text/javascript" src="DrawClicks.js"></script>
<script type="text/javascript" src="DrawImpression.js"></script>
<script type="text/javascript" src="DrawMargin.js"></script>
<script type="text/javascript" src="DrawCtr.js"></script>
<script type="text/javascript" src="DrawCharts.js"></script>
<script type="text/javascript">

// Load the Visualization API and the piechart package.
google.load('visualization', '1', {'packages':['corechart']});


window.onload = function() {
	document.getElementById("chart_div").innerHTML = "";
	var query = QueryString();
	if(!query || query.length == 0)
	{
		document.getElementById("chart_div").innerHTML = "<h2>View Real Time Data Charts</h2>";
	} else {
		LoadCharts(query, 0);
	}
} 
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
		<table><tbody><tr>
		
			<td><div id="inputform">
				 <h3>View Real Time Data</h3>
				 <form method="GET" action="index.php">
    				Publisher ID: 
						<select name="publisher">
  							<option value="">ALL</option>
							<?php
   								for ($i = 0; $i < 50; $i++) {
      								print "<option value=\"$i\">Publisher $i</option>\n";
    							}
							?>
						</select><br><br>
   				 	Advertiser ID: 
						<select name="advertiser">
  							<option value="">ALL</option>
							<?php
    							for ($i = 0; $i < 200; $i++) {
      								print "<option value=\"$i\">Advertiser $i</option>\n";
    							}
							?>
						</select><br><br>
    				Ad Unit: 
    					<select name="adunit">
      						<option value="" selected="selected">ALL</option>
							<?php
    							for ($i = 0; $i < 5; $i++) {
      								print "<option value=\"$i\">Adunit $i</option>\n";
    							}
						?>
    					</select> <br><br>
    				Chart Window: 
					<select name="chartwindow">
						<option value="hour" selected="selected">HOUR</option>
						<option value="day">DAY</option>
						<option value="week">WEEK</option>
    					</select> <br><br>
				Refresh Interval : <input type="textfield" size="4" name="refreshinterval" value="5" />Secs
    				<br><br><input type="submit" name="submit" value="Submit" />
  				</form>
			</div></td>
			
    		<td>
			<table><tbody>
                           <tr>
	               		<td><div id="chart_div" ></div></td>	
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
		</td>
		</tr></tbody></table>
	</div>		
</body>
</html>
