# Release Notes
2023-10-31

## Added

* oml-384 Added the capability to set the NAT ip address for PJSIP trunk-transport

## Changed

* oml-345 The Dockerfile and docker-entrypoint.sh files have been modified to optimize the container startup process
* oml-345 logger.conf was modified to only keep logs on stdout
* oml-304 The streaming of manager events related to app_queue.so has been enabled
* oml-384 Cloud scenary now attach 5060 to public ip

## Fixed

* oml-328 ENV=all now enable the manager user on 0.0.0.0
* oml-310 Now the default music on hold is working correctly
* oml-2487 Inbound and manual call transfer to campaign failure reports
* oml-2509 Inbound CTOUT BUSY/CONGESTION failure reports

## Removed

* oml-345 master.csv & /var/log/asterisk/full file logs
* oml-389 The connection between Asterisk ODBC and Postgres has been removed, leaving the task of logging to the FastAGI and omalpp components

