// cross browser safe JSON parsing
// illegal JSON stops here ...
(function (window, undefined) {
  // taken from http://json.org/json2.js
  var ok_json = function ( data ) {
	      return /^[\],:{}\s]*$/.test(
	    data.replace(/\\(?:["\\\/bfnrt]|u[0-9a-fA-F]{4})/g, "@")
	    .replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g, "]")
	    .replace(/(?:^|:|,)(?:\s*\[)+/g, "")
	  ) ;
  };

  // true if native JSON exists and supports non-standard JSON
  var ok_wrong_json = function () {
    try { JSON.parse("{ a : 1 }"); return true ; } catch(x) { return false ; }
  }();

  window.json_parse =
    ( window.JSON && ("function" === typeof window.JSON.parse) ) ?

    ( ok_wrong_json ) ?
    function json_parse ( data ) {
    // Case 1 : native JSON is here but supports illegal strings
    if ( ! ok_json( data ) ) throw new Error(0xFFFF,"Bad JSON string.") ;
    return window.JSON.parse( data ) ;
  }
  : // else
  function json_parse ( data ) {
    // Case 2: native JSON is here , and does not support illegal strings
    // this will throw on illegal strings
    return window.JSON.parse( data ) ;
  }
  : // else
  function json_parse ( data ) {
    // Case 3: there is no native JSON present
    if ( ! ok_json( data ) ) throw new Error(0xFFFF,"Bad JSON string.") ;
    return (new Function("return " + data))();
  }
  ;
})(window) ;


function LongPoll(uri, callback) {
  function getXMLHttpRequestObject() {
    var ref = null;
    if (window.XMLHttpRequest) {
      ref = new XMLHttpRequest();
    } else if (window.ActiveXObject) { // Older IE.
      ref = new ActiveXObject("MSXML2.XMLHTTP.3.0");
    }
    return ref;
  }

    var xhr = getXMLHttpRequestObject();
    xhr.async = true;
    if (callback) {
      var previousLength = 0;
      var chunk = "";

      function processChunk() {
        // next chunk header
        try {
          if (chunk) {
            jsonObject = window.json_parse(chunk);
            callback(jsonObject);
          }
        }
        catch (x) {
          console.log("Error " + x + " while processing " + xhr.responseText.substring(previousLength));
        }
        // advance response index (also skip on parse error to not block forever)
        chunk = "";
        previousLength = xhr.responseText.length;
      }
      
      
      xhr.onreadystatechange = function() {
        //console.log("response *" + xhr.responseText.substring(previousLength) + "* " + xhr.readyState);
        if (xhr.readyState == 3) {
          var responseLines = xhr.responseText.substring(previousLength).match(/[^\r\n]+/g);
          if (responseLines == null) {
             //console.log("no response lines");
             return;
          }  
          for (var i = 0; i < responseLines.length; i++) {
            var line = responseLines[i];
            if (isNaN(line)) {
              // chunk content
              //console.log("content: " + line);
              chunk += line;
            } else {
              // next chunk header
              processChunk();
            }
          }

          if (chunk) {
            processChunk();
          }
          
        }
      };
    }
    xhr.open("GET", uri, true);
    xhr.setRequestHeader('x-stream', 'rockon');
    xhr.send();

}
