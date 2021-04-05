# Asterisk for OMniLeads

This repository has the code of Asterisk component, configuration used for OMniLeads

## Docker image

* **Asterisk Version:** 16.16.2
* **Base Image:** freetechsolutions/omlacd-builder:16.16.2

### Build

Asterisk image is based on the ACD builder. Is the base that will build all the binaries and libraries of asterisk. To build it:
```
  cd build/docker/acdbuilder
  DOCKER_USER=$USER DOCKER_PASSWORD=$PASSWORD build_images.sh builder
```
Where $USER and $PASSWORD are credentials of docker repository.

After building this, you can build the omlacd image.
```
  cd build/docker
  docker build -f Dockerfile -t freetechsolutions/omlacd:$TAG ../..
```
Where $TAG is the docker tag you want for image. You can check the .package_version file for the tag.

### Run container

```
  docker run -it freetechsolutions/omlacd:latest bash
```

If you need to add environment variables and link folders to container, check docker run documentation: https://docs.docker.com/engine/reference/commandline/run/

**Environment variables needed:**
```
  AMI_USER // user of OMniLeads AMI
  AMI_PASSWORD // password for OMniLeads AMI user
  DOCKER_IP // IP of docker host
  PGHOST= // host of postgresql
  PGPORT= // port of postgresql
  PGDATABASE // database of postgresql
  PGPASSWORD= // password of postgresql
  PGUSER // user pof postgresql
  REDIS_HOSTNAME // hostname of redis service
  TZ // timezone that will have the container
```

## RPM

### Build

* **Asterisk version:** The Asterisk base version is written in file `.asterisk_version`
* **Package version:** We provide the package with all the files configured for using Asterisk with OMniLeads. The version of the package is in `.package_version` file

Test the RPM build with these steps:

1. Check variables for container builder in `scripts/.env_buildercontainer` file.
2. Cd into build/rpm
3. Run builder_container.sh script
4. Inside the container, cd again into build/rpm
5. Execute build_rpm.sh script

### Deploy

To deploy Asterisk in a dedicated host two main steps are needed:

1. Install OMniLeads in its host, editing the parameter `asterisk_host` with the IP or hostname of the machine where Asterisk will be installed.
2. Install Asterisk in its host, following these steps:

**SO:** Centos7 and derivatives

* Update the machine
```
  yum update -y
```
* Set timezone in accordance where you need it
* Disable selinux if enabled
```
  sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/sysconfig/selinux
  sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
```
* Disable firewalld if enabled
```
  systemctl disable firewalld
  systemctl stop firewalld
```
* Reboot the machine
* Install git
```
  yum install git -y
```
* Clone this repository where you want
```
  git clone https://gitlab.com/omnileads/omlacd.git
```
* Install ansible in the dedicated host.
```
  yum install python3-pip python3 epel-release -y
  pip3 install pip --upgrade
  pip3 install 'ansible==2.9.2'
```
* Go to `deploy` directory
```
  cd omlacd/deploy
```
* Open the file ansible/inventory and set there the parameters.
```
  [prodenv-aio:vars]
  ## IP or hostnames of services that interact with asterisk       ###
  ## WARNING: if you use hostnames you manage the hostname resolve ###
  asterisk_hostname=asterisk
  kamailio_hostname=kamailio
  redis_hostname=redis
  rethinkdb_hostname=rethinkdb
  rtpengine_hostname=rtpengine
  postgres_hostname=postgres
  postgres_port=5432
  postgres_user=omnileads
  postgres_password=my_very_strong_pass

  # ami credentials
  ami_user=omnileads
  ami_password=C12H17N2O4P_o98o98
```

* Run ansible-playbook   
```
  ansible-playbook asterisk.yml -i inventory --extra-vars "repo_location=$(pwd)/.. asterisk_version=$(cat ../.package_version)"
```
---
**NOTE**

* If asterisk can't access redis and rtpengine services, asterisk service will not start.
---
