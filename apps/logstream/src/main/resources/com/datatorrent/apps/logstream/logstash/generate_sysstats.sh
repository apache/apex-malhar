#!/bin/bash
while sleep 1; 
do (
  # current timestamp
  echo -n "timestamp: " >> /location/to/log/stats1.log; 
  date +%s000 >> /location/to/log/stats1.log; 
  # mem info
  cat /proc/meminfo >> /location/to/log/stats1.log;
  # cpu info
  cat /proc/stat | grep "cpu " >> /location/to/log/stats1.log; 
  # disk info
  cat /proc/diskstats | grep "sda " >> /location/to/log/stats1.log;
); 
done
