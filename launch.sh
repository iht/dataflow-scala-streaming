#!/bin/sh

PROJECT_ID=$1
REGION=europe-west1
TMP_BUCKET=gs://${PROJECT_ID}-tmp
DATASET_NAME=meetup_sandbox

# Sanitize JAR names
ls target/pack/lib/*\[* | while read l
          do
            mv $l `echo $l | sed 's/\[/_/g' | sed 's/\]/_/g'`

          done

bq rm -f ${DATASET_NAME}.point_rides
bq rm -f ${DATASET_NAME}.total_rides
bq rm -f ${DATASET_NAME}.errors

cd target/pack/bin
./madrid-meetup-streaming-pipeline --project=$PROJECT_ID --region=$REGION \
            --runner=DataflowRunner --gcpTempLocation=${TMP_BUCKET}/dataflow-tmp \
            --pubsub-topic=projects/pubsub-public-data/topics/taxirides-realtime \
            --output-table="${PROJECT_ID}:${DATASET_NAME}.point_rides" \
            --accum-table="${PROJECT_ID}:${DATASET_NAME}.total_rides" \
            --errors-table="${PROJECT_ID}:${DATASET_NAME}.errors"