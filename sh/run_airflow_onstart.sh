#!/bin/bash

# start airflow server on 8080 port

cd ~
cd airflow
source airflow/bin/activate
nohup airflow webserver -p 8080 &
nohup airflow scheduler &
