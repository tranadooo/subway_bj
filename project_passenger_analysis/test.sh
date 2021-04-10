#!/bin/sh
#thie script will be used to analyse the commuter passenger for some line
handle/a_txtToDB.py
handle/b_ic2TripChain.py
handle/c_TripChain2CommuterChain.py 
handle/d_handleCommuterLawData.py

analyse/e_scoreCommuterLaw.py
analyse/f_clusterPassenger.py >>logs/cluster.log
