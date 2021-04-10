#!/bin/sh
#thie script will be used to analyse the commuter passenger for some line
#before run it,configure the config.py
#then use this script like:commuter_analysis_manager.sh
path=$(cd "$(dirname "$0")";pwd)
prepare/prepare_db.py

python3 -u handle/a_txtToDB.py
python3 -u handle/b_ic2TripChain.py
python3 -u handle/c_TripChain2CommuterChain.py 
python3 -u handle/d_handleCommuterLawData.py

python3 -u analyse/e_scoreCommuterLaw.py
python3 -u analyse/f_clusterPassenger.py >>logs/cluster.log

