#!/bin/bash
mvn package
scp target/TP2Hadoop-0.1.jar pcordonnier@ns3024382.ip-149-202-81.eu:TP.jar
ssh pcordonnier@ns3024382.ip-149-202-81.eu ./hadoop.sh
scp -r pcordonnier@ns3024382.ip-149-202-81.eu:out/ ./
