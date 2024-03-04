#!bin/bash

BASIC='\033[1;35m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
POINT='\033[0;33m'
NC='\033[0m'


START_YEAR=2019
END_YEAR=2023


echo "==================================="
echo -e "${BASIC}Running Year-wise (2019-2023)${NC}"
echo -e "To edit the range change parameters in ${POINT}run.sh${NC} file"


## HIRE

echo "==================================="
echo -e "${POINT}Running DLT process: Hire Taxi Data${NC}"

for (( year=2019; year<=END_YEAR; year++ ))
do
    echo -e "${BASIC}YEAR${NC}" $year
    echo
    python3 nyc_taxi.py --year $year --taxi H
done

echo -e "${POINT}DONE DLT process: Hire Taxi Data${NC}"
echo "==================================="

echo "DLT Process Complete"
