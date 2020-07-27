# The Ursprung Provenance Collection System

This is the main repository for the Ursprung provenance collection system.
The system consists of the collection backend (which includes the auditd
plugin, the provd daemon, and the kafka consumer) and the GUI.

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
5. Start provd `docker run -v /opt/ursprung/config/:/opt/ursprung/config/ -it ursprung-collection-system /opt/collection-system/build/provd/provd /opt/ursprung/config/provd.cfg`
5. Start GUI Backend: `docker run -v /opt/ursprung/:/opt/ursprung/ --network host -it ursprung-gui node /opt/gui/backend/app.js`
6. Start GUI Frontend: `docker run -p 3000:3000 -it ursprung-gui /bin/bash -c "cd /opt/gui/frontend; npm start"` 
