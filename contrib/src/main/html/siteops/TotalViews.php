<?php
header("Content-type: application/json");
$redis = new Redis();
$redis->connect('127.0.0.1');
$redis->select(11);

$value = $redis->get(1);
print $value;
?>
