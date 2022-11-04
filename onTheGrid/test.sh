#!/bin/bash 

WDIR="/opt/ppd/darkmatter/migdal/dataTransfer"
  
echo Payload Starting - `date` 
echo Payload Arguments: 
echo $@ 

cd ${WDIR}
export X509_USER_PROXY=${WDIR}/proxy.proxy
source dTransfer/bin/activate
cd onTheGrid
echo "LFN to zip and upload : " $1
python3 zipAndGo.py "$1"

echo Running Payload 
mkdir local_output 
# cp $1 local_output/$2 
# cp $1 local_output/$2.unwanted 
