# The Ursprung Provenance Collection System

The Ursprung provenance collection system is a flexible provenance collection framework +
a GUI for tracking machine learning and data science experiments and pipelines in a
cluster.

The collection framework
combines low-level provenance information from system sources (operating and file system)
with application-specific provenance that can be collected through rules in Ursprung's
rule language.

The GUI allows users to navigate the provenance graph and has additional features to
view and compare past pipeline executions.

Ursprung is currently in pre-alpha phase.

## Architecture overview

**System requirements**

Ursprung currently only supports Linux and requires a
[IBM Spectrum Scale](https://www.ibm.com/products/scale-out-file-and-object-storage) file system
to generate file system notifications. The recommended Linux distribution is RedHat or Centos.

**Components**

Ursprung consists of six main components:
1. The provenance consumers
2. The provenance GUI
3. The provenance database
4. The `provd` provenance daemons
5. An [auditd](https://man7.org/linux/man-pages/man8/auditd.8.html) pluging to collect operating system events through Linux's auditing subsystem
6. A Kafka message queue

Below is an overview of how the different components interact with each other.

![Ursprung Architecture](doc/architecture.svg?raw=true)

## Building the container images

All Dockerfiles are located in `deployment`.

- Base: `docker build -f Dockerfile.ursprung.build-base -t ursprung-base ../`
- Database: `docker build -f Dockerfile.ursprung.db -t ursprung-db .`
- Consumers: `docker build -f Dockerfile.ursprung.collection-system -t ursprung-collection-system ../`
- GUI: `docker build -f Dockerfile.ursprung.gui -t ursprung-gui ../`

## Running the components

1. Add cluster-specific Kafka configuration to the consumer config templates under `deployment/kafka`
2. Copy consumer configs to `/opt/ursprung/config`
3. Start DB container: `docker run -v /opt/ursprung/data:/var/lib/postgresql/data -p 5432:5432 -it ursprung-db`
4. Start Consumers:
    * Scale: `docker run -v /opt/ursprung/:/opt/ursprung/ --network host -it ursprung-collection-system /opt/collection-system/build/consumer/prov-consumer -c /opt/ursprung/config/scale-consumer.cfg`
    * Auditd: `docker run -v /opt/ursprung/:/opt/ursprung/ --network host -it ursprung-collection-system /opt/collection-system/build/consumer/prov-consumer -c /opt/ursprung/config/auditd-consumer.cfg`
5. Start provd `docker run -v /opt/ursprung/:/opt/ursprung/ -v /tmp/:/tmp/ --network host --pid host --privileged -it ursprung-collection-system /opt/collection-system/build/provd/provd /opt/ursprung/config/provd.cfg`
5. Start GUI Backend: `docker run -v /opt/ursprung/:/opt/ursprung/ --network host -it ursprung-gui node /opt/gui/backend/app.js`
6. Start GUI Frontend: `docker run -p 3000:3000 -it ursprung-gui /bin/bash -c "cd /opt/gui/frontend; npm start"` 
