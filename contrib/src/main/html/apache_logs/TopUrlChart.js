/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
 */

function DrawTopUrlTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        topUrlTable = new google.visualization.DataTable();
        topUrlTable.addColumn('string', 'TopUrl');
        for(var i=0; i <  pts.length; i++) 
        {
          var row = new Array();
          row.push(pts[i]);
          topUrlTable.addRow(row);
          delete row;
          delete pts[i];
        }
        //document.getElementById('top_url_div').innerHTML = data;
        topUrlTableChart.draw(topUrlTable, {showRowNumber: true});
        delete topUrlTable;
        delete data;
        delete pts;
      }
    }
    connect.open('GET',  "TopUrlData.php", true);
    connect.send(null);
  } catch(e) {
  }
}
