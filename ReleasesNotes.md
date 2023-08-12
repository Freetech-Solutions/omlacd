# Release Notes
2023-08-11

## Added

* oml-244 back & restore procedure with python scripts
* oml-307 new ENV scenary (ENV=ha) in order to attach SIP 5060 & AMI 5038 to cluster VIP

## Changed

* oml-305 the call log registration logic from ODBC queue_log has been extrapolated to FastAGI
* oml-305 move all python scripts to new location /opt/asterisk/scripts
* oml-305 upgrade to asterisk 18.19.0

## Fixed

* oml-308  New environment variable (UTC_LOGS) to indicate whether to keep logs (queue_log) using UTC

## Removed

No removals in this release.
