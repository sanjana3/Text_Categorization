#!/bin/bashsudo yum install -y python36-devel python36-pip python36-setuptools python36-virtualenvsudo python36 -m pip install --upgrade pip
#
sudo python36 -m pip install pandas
#
sudo python36 -m pip install boto3
#
sudo python36 -m pip install re
#
sudo python36 -m pip install spark-nlp==2.4.5