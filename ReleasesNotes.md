# Release Notes
2025-04-10

## Added

* Implemented a new dial plan to support multiple call attempts to different numbers.
* Introduced a new dial plan specifically for the Omnidialer module, enhancing its functionality.

## Changed

* Upgraded Asterisk to version 20.12.0, incorporating the latest features and security updates.
* Modified the Docker entrypoint to ensure proper functionality when using the bridge network mode.

## Fixed

* Audio Path Configuration:** Corrected issues related to default audio paths in Asterisk and improved the upload process for custom audio prompts.
* Call termination occurs when a DTMF digit is detected during the welcome announcement in the call queue."

## Removed

* Removed the `oml_asterisk.conf` include, streamlining the configuration process.
