# ACD for OMniLeads

This repository has the code of ACD component, configuration used for OMniLeads

Asterisk Version: 16.12.0
Base Image: freetechsolutions/omlacd-builder:latest

## Build

```
  docker build -t freetechsolutions/omlacd:$TAG .
```

Where $TAG is the tag you want for the image

## Run container

```
  docker run -it freetechsolutions/omlacd:latest bash
```

If you need to add environment variables and link folders to container, check docker run documentation: https://docs.docker.com/engine/reference/commandline/run/
