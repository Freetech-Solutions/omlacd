# Release Notes
2025-03-13

## Added

* **Multinumber Call Attempts:** Implemented a new dial plan to support multiple call attempts to different numbers.
* **Omnidialer Module Integration:** Introduced a new dial plan specifically for the Omnidialer module, enhancing its functionality.

## Changed

* **Asterisk Upgrade:** Upgraded Asterisk to version 20.12.0, incorporating the latest features and security updates.
* **Docker Network Mode (Bridge):** Modified the Docker entrypoint to ensure proper functionality when using the bridge network mode.

## Fixed

* **Audio Path Configuration:** Corrected issues related to default audio paths in Asterisk and improved the upload process for custom audio prompts.

## Removed

* **oml_asterisk.conf Inclusion:** Removed the `oml_asterisk.conf` include, streamlining the configuration process.
