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
echo -e "To edit the range change parameters in ${POINT}run*.sh${NC} file"


## GREEN

echo "==================================="
echo -e "${GREEN}Running DLT process: Green Taxi Data${NC}"

for (( year=START_YEAR; year<=END_YEAR; year++ ))
do
    echo -e "${BASIC}YEAR${NC}" $year
    echo
    python3 nyc_taxi.py --year $year --taxi G
done

echo -e "${GREEN}DONE DLT process: Green Taxi Data${NC}"
echo "==================================="