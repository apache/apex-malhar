/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
 */

function DrawUrl404TableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        var url404Table = new google.visualization.DataTable();
        url404Table.addColumn('string', 'Url 404 Status');
        for(var i=0; i <  pts.length; i++) 
        {
          var row = new Array();
          row.push(pts[i]);
          url404Table.addRow(row);
        }
        //document.getElementById('risky_client_div').innerHTML = data;
        //document.getElementById('risky_client_div').innerHTML = url404Table.getNumberOfRows();
        url404TableChart.draw(url404Table, {showRowNumber: true});
        delete url404Table;
        delete data;
        delete pts;
      }
    }
    connect.open('GET',  "Url404.php", true);
    connect.send(null);
  } catch(e) {
  }
}
