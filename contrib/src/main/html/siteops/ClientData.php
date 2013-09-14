<?php
header("Content-type: application/json");
$redis = new Redis();
$redis->connect('127.0.0.1');
$redis->select(9);

// result array
$result = array();

$value = $redis->get(1);
$result[] = $value;

print json_encode($result);

?>
