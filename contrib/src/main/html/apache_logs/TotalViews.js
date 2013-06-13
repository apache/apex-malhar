/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
 */

function DrawTotalViewsChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        //document.getElementById('total_views_div').innerHTML = data;
        var totalViewsTable = new google.visualization.DataTable();
        totalViewsTable.addColumn('string', 'Total Page Views');
        var row = new Array();
        row.push(data);
        totalViewsTable.addRow(row);
        totalPageViewsChart.draw(totalViewsTable, {showRowNumber: true});
        delete totalViewsTable;
        delete data;
        delete row;
      }
    }
    connect.open('GET',  "TotalViews.php", true);
    connect.send(null);
  } catch(e) {
  }
}
