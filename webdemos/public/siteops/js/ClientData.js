/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 */

function DrawClientDataTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        document.getElementById('totaldata').innerHTML = data;
      }
    }
    connect.open('GET',  "clientData", true);
    connect.send(null);
  } catch(e) {
  }
}
