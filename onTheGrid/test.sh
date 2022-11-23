#!/bin/bash 

export MIG_DIR="/opt/ppd/darkmatter/migdal/dataTransfer"
 
echo Payload Starting - `date` 
echo Payload Arguments: 
echo $@ 

# cd ${MIG_DIR}
export X509_USER_PROXY=${MIG_DIR}/proxy.proxy
source ${MIG_DIR}/dTransfer/bin/activate
# cd ${TMPDIR}
echo "LFN to zip and upload : " $1
python3 ${MIG_DIR}/onTheGrid/zipAndGo.py "$1"
