/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
    
/**
 * Creates data table with time stamp and ctr values.
 * Draw line chart for time vs ctr.
 * @author Dinesh Prasad (dinesh@malhar-inc.com).
 */
function DrawCtrChart(dataArr)
{
  // Populate data table with time/ctr data points. 
  var dataTable = new google.visualization.DataTable();
  dataTable.addColumn('date', 'Time');
  dataTable.addColumn('number', 'Impressions');
  dataTable.addRows(dataArr.length);
  for(var i=0; i < dataArr.length; i++)
  {
    dataTable.setCell(i, 0, new Date(parseInt(dataArr[i].timestamp)));
    if (parseInt(dataArr[i].impressions) == 0 )
    {      
      dataTable.setCell(i, 1, 0);
    } else {
      dataTable.setCell(i, 1, (parseInt(dataArr[i].clicks)/parseInt(dataArr[i].impressions))*100);
    }
  }

  // Format time column in readable value.
  var formatter3 = new google.visualization.DateFormat({pattern: "h:m:s aa", timeZone: -7});
  formatter3.format(dataTable, 0);
  //document.getElementById("chart_div").innerHTML = dataTable.toJSON();

  // Draw line chart.
  var dataView = new google.visualization.DataView(dataTable);
  dataView.setColumns([{calc: function(data, row) { return data.getFormattedValue(row, 0); }, type:'string'}, 1]);
  var chart = new google.visualization.LineChart(document.getElementById('chart5_div'));
  var options = {
    title: 'CTR Chart',
    width: 500, height: 300,
    legend: 'none',
    pointSize: 3,
  };
  chart.draw(dataView, options);
}
