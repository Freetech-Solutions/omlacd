# Release Notes
2023-10-01

## Added

* No added in this release.

## Changed

* oml-345 The Dockerfile and docker-entrypoint.sh files have been modified to optimize the container startup process
* oml-345 logger.conf was modified to only keep logs on stdout
* oml-304 The streaming of manager events related to app_queue.so has been enabled

## Fixed

* oml-328 ENV=all now enable the manager user on 0.0.0.0
* oml-310 Now the default music on hold is working correctly
* oml-2487 Inbound and manual call transfer to campaign failure reports
* oml-2509 Inbound CTOUT BUSY/CONGESTION failure reports

## Removed

* master.csv & /var/log/asterisk/full file logs
