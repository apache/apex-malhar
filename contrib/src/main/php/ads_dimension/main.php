<?php

if ($_GET['submit'] == 'Submit') {
  $redis = new Redis();
  $redis->connect('127.0.0.1');

  $key_pattern = $_GET['time'] . '|*|';

  $arr = array();
  
  if ($_GET['publisher'] != '') {
    $arr[] = "0:".$_GET['publisher'];
  }
  if ($_GET['advertiser'] != '') {
    $arr[] = "1:".$_GET['advertiser'];
  }
  if ($_GET['adunit'] != '') {
    $arr[] = "2:".$_GET['adunit'];
  }
  if (count($arr) == 0) {
    // slow
    $key_pattern .= '*';
  } else {
    $key_pattern .= join("|", $arr);
  }

  $keys = $redis->keys($key_pattern);
  $count = array();
  $cost = array();

  foreach ($keys as $key) {
    if (preg_match('/[a-z]\|(\d+)\|/i', $key, $matches)) {
      $time = $matches[1];
      $hash = $redis->hGetAll($key);
      $count[$time] += $hash['0'];
      $cost[$time] += $hash['1'];
    }
  }

  ksort($count);
  ksort($cost);

  echo "Count:<br/>";
  
  foreach ($count as $time => $val) {
    echo "$time => $val<br/>";
  }

  echo "<p/>";

  echo "Cost:<br/>";
  foreach ($cost as $time => $val) {
    echo "$time => $val<br/>";
  }


} else {
?>

<body>
  <form method="GET">
    Publisher ID: 
<select name="publisher">
  <option value="">ALL</option>
<?php
    /*
  $file = fopen("publishers.txt", "r");
  while (!feof($file)) {
    $line = trim(fgets($file));
    if ($line != '') {
      print "<option value=\"$line\">$line</option>\n";
    }
  }
  fclose($file);
    */
    for ($i = 0; $i < 50; $i++) {
      print "<option value=\"$i\">Publisher $i</option>\n";
    }
?>
</select>
    <p/>
    Advertiser ID: 
<select name="advertiser">
  <option value="">ALL</option>
<?php
    /*
  $file = fopen("advertisers.txt", "r");
  while (!feof($file)) {
    $line = trim(fgets($file));
    print "<option value=\"$line\">$line</option>\n";
  }
  fclose($file);
    */
    for ($i = 0; $i < 200; $i++) {
      print "<option value=\"$i\">Advertiser $i</option>\n";
    }
?>
</select>
    <p/>
    Ad Unit: 
    <select name="adunit">
      <option value="" selected="selected">ALL</option>
<?php
    /*
      <option value="wsky">wsky</option>
      <option value="ldr">ldr</option>
      <option value="mrec">mrec</option>
    */
    for ($i = 0; $i < 5; $i++) {
      print "<option value=\"$i\">Adunit $i</option>\n";
    }
?>
    </select>
    <p/>
    Time Bucket: 
    <input type="radio" name="time" value="D" checked="checked"/>Day
    <input type="radio" name="time" value="h"/>Hour
    <input type="radio" name="time" value="m"/>Minute
    <p/>
    <input type="submit" name="submit" value="Submit" />
  </form>
</body>

<?php
}
?>