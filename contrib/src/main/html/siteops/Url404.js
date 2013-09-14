/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
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
        url404Table.addColumn('string', 'URL');
        url404Table.addColumn('number', '404/sec');
        url404Table.addRows(10);
        for(var i=0; ((i <  pts.length)&&(i < 10)); i++) 
        {
          var row = pts[i].split("##");
          if ((row[1] == null)||(row[1] == ""))
          {
            url404Table.setCell(i, 0,  "-");
          } else {
            url404Table.setCell(i, 0, row[0]);
          }
          if ((row[1] == null)||(row[1] == ""))
          {
            url404Table.setCell(i, 1, 0);   
          } else {
            url404Table.setCell(i, 1, parseInt(row[1]));
          }
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
