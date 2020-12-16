#!/bin/bash
# For 30th November

# For 1 December to 14 December
for i in {30..30}
do
    for j in {07..19}
    do
        echo "Welcome $i $j times"
        elasticdump --input=http://10.4.130.10:9200/traffic_flow --output=raw_kibana/traffic_flow_${i}112020_${j}00_${j}59.json --type=data --ignore-errors=true --size=-1 --searchBody "{\"query\": {\"range\":{\"PBT\":{\"gte\":\"2020-11-${i}T${j}:00:00\",\"lte\":\"2020-11-${i}T${j}:59:59\"}}}}"
    done
done



# For 1 December to 14 December
for i in {01..14}
do
    for j in {07..19}
    do
        echo "Welcome $i $j times"
        elasticdump --input=http://10.4.130.10:9200/traffic_flow --output=raw_kibana/traffic_flow_${i}122020_${j}00_${j}59.json --type=data --ignore-errors=true --size=-1 --searchBody "{\"query\": {\"range\":{\"PBT\":{\"gte\":\"2020-12-${i}T${j}:00:00\",\"lte\":\"2020-12-${i}T${j}:59:59\"}}}}"
    done
done