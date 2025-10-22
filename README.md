# OMniLeads ACD Component (Automatic Call Distributor)

## Overview

This repository contains the core **Automatic Call Distributor (ACD)** component for the Omnileads platform. Based on **Asterisk**, this module is responsible for managing all voice call traffic, including queueing, agent distribution, and execution of dialplan logic for both inbound and outbound campaigns.

This component is designed to be run as a containerized service, orchestrated by the `omldeploytool` as part of a complete Omnileads deployment.

## Technology Stack

*   **Telephony Engine:** Asterisk `20.14.0` (https://gitlab.com/omnileads/asterisk_base_img)
*   **Containerization:** Docker or Podman

## Component Structure

The repository is organized as follows:

```
.
├── build/              # Scripts and resources for building the component
├── docs/               # Additional documentation
├── source/             # Core source files, including Asterisk configurations
├── .gitlab-ci.yml      # GitLab CI/CD pipeline definition
├── Dockerfile          # Dockerfile for building the service image
├── README.md           # This documentation file
```

## Deployment

This component is not intended for standalone deployment. It is built into a Docker image using the provided `Dockerfile` and deployed as a service within the Omnileads ecosystem. The entire lifecycle, from build to deployment and configuration, is managed by the `omldeploytool`.

## Versioning

*   The component version is tracked with git `tags`.
