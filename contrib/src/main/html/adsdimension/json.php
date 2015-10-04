<?php
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
header("Content-type: application/json");
$redis = new Redis();
$redis->connect('localhost');
$from = $_GET['from'];
$bucket = $_GET['bucket'];
$publisher = $_GET['publisher'];
$advertiser = $_GET['advertiser'];
$adunit = $_GET['adunit'];

switch ($bucket) {
case 'D':
  $format = 'Ymd';
  $incr = 60 * 60 * 24;
  break;
case 'h':
  $format = 'YmdH';
  $incr = 60 * 60;
  break;
case 'm':
  $format = 'YmdHi';
  $incr = 60;
  break;
default:
  break;
}

$arr = array();
if ($publisher != '') {
  $arr[] = "0:".$publisher;
} 
if ($advertiser != '') {
  $arr[] = "1:".$advertiser;
} 
if ($adunit != '') {
  $arr[] = "2:".$adunit;
} 
$subpattern = "";
if (count($arr) != 0) {
  $subpattern = join("|", $arr);
}

$result = array();

while ($from < time()) {
  $date = gmdate($format, $from);
  if ($subpattern != '') {
    $key = $bucket . '|' . $date . '|' . $subpattern;
  } else {
    $key = $bucket . '|' . $date ;
  }
  $hash = $redis->hGetAll($key);
  if ($hash) {
    $cost = $hash['1'];
    $revenue = $hash['2'];
    $impressions = $hash['3'];
    $clicks = $hash['4'];
    $result[] = array('timestamp'=> $from * 1000, 'cost'=>$cost, 'revenue'=>$revenue, 'clicks'=>$clicks, 'impressions'=>$impressions);
  }
  $from += $incr;
}

array_pop($result);
print json_encode($result);

?>
