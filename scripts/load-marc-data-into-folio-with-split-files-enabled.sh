#!/bin/sh

# First log into a back-end FOLIO system. storing the Okapi URL,
# tenant and token in the file `.okapi` in the home directory:
#
#    OKAPI_URL=https://folio-snapshot-stable-okapi.dev.folio.org
#    OKAPI_TENANT=diku
#    OKAPI_TOKEN=123abc
#
# You can conveniently do this using `okapi login` with this CLI:
# https://github.com/thefrontside/okapi.rb
#
# Then invoke as: ./load-marc-data-into-folio-with-file-splitting-enabled.sh sample100.mrc

# Below JOBPROFILENAME is "Default - Create instance and SRS MARC Bib" but URL encoded.
JOBPROFILENAME="Default%20-%20Create%20instance%20and%20SRS%20MARC%20Bib"

if [ $# -ne 1 ]; then
   echo "Usage: $0 <MARCfile>" >&2
   exit 1
fi
filename="$1"
if [ ! -f "$filename" ]; then
    echo "$0: no such MARC file: $filename" >&2
    exit 2
fi

filesize=$(wc -c < $1)
filesize_kb=$((filesize / 1024))


. ~/.okapi
tmpfile1=`mktemp`
tmpfile2=`mktemp`
tmpfile3=`mktemp`
tmpfile4=`mktemp`
tmpfile5=`mktemp`
tmpfile6=`mktemp`
tmpfile7=`mktemp`
tmpfile8=`mktemp`
trap 'rm -f $tmpfile1 $tmpfile2 $tmpfile3 $tmpfile4 $tmpfile5 $tmpfile6 $tmpfile7 $tmpfile8' 1 15 0
echo "Size of $filename =$filesize_kb kilobytes"
#echo "Using OKAPI_URL: $OKAPI_URL"
#echo "Token: $OKAPI_TOKEN"
#echo "Tenant: $OKAPI_TENANT"


echo "=== 1. Create upload definition ==="
curl --silent --location --request POST "$OKAPI_URL/data-import/uploadDefinitions" \
    --header "Content-Type: application/json" \
    --header "X-Okapi-Tenant: $OKAPI_TENANT" \
    --header "X-Okapi-Token: $OKAPI_TOKEN" \
    --data-raw "{ \"fileDefinitions\": [{ \"name\": \"$filename\", \"size\": $filesize_kb }] }" \
        > $tmpfile1

uploadDefinitionId=`jq -r -M .id $tmpfile1`
fileDefinitionId=`jq -r -M '.fileDefinitions[0].id' $tmpfile1`
 echo "uploadDefinitionId=$uploadDefinitionId"
 echo "fileDefinitionId=$fileDefinitionId"
 echo
sleep 20

echo "=== 2. Request upload URL ==="
curl --silent --location --request GET "$OKAPI_URL/data-import/uploadUrl?filename=$filename" \
    --header "Accept: application/json" \
    --header "X-Okapi-Tenant: $OKAPI_TENANT" \
    --header "X-Okapi-Token: $OKAPI_TOKEN" \
        > $tmpfile2
UPLOADURL=`jq -r -M .url $tmpfile2`
UPLOADID=`jq -r -M .uploadId $tmpfile2`
UPLOADKEY=`jq -r -M .key $tmpfile2`
 echo "UPLOADURL=$UPLOADURL"
 echo "UPLOADID=$UPLOADID"
 echo "UPLOADKEY=$UPLOADKEY"
 echo
sleep 10

echo "=== 3. Upload file ==="
curl --silent --location --request PUT "$UPLOADURL" \
    -D $tmpfile3 \
    --data-binary "@$filename" \
        > $tmpfile4
ETAG=`grep -i 'etag' $tmpfile3 | cut -d ':' -f2`
echo "ETAG=$ETAG"
echo
sleep 10

echo "=== 4. Request file to be assembled for import ==="
curl --silent --location --request POST "$OKAPI_URL/data-import/uploadDefinitions/$uploadDefinitionId/files/$fileDefinitionId/assembleStorageFile" \
    --header "Content-Type: application/json" \
    --header "X-Okapi-Tenant: $OKAPI_TENANT" \
    --header "X-Okapi-Token: $OKAPI_TOKEN" \
    --data-raw "{\"uploadId\": \"$UPLOADID\", \"key\": \"$UPLOADKEY\", \"tags\": [$ETAG]}" \
        > $tmpfile5
sleep 5

echo "=== 5. Get fresh copy of upload definition ==="
curl --silent --location --request GET "$OKAPI_URL/data-import/uploadDefinitions/$uploadDefinitionId" \
    --header "Accept: application/json" \
    --header "X-Okapi-Tenant: $OKAPI_TENANT" \
    --header "X-Okapi-Token: $OKAPI_TOKEN" \
        > $tmpfile6

echo "=== 6. Get the job profile information ==="
curl --silent --location --request GET "$OKAPI_URL/data-import-profiles/jobProfiles?query=name==\"$JOBPROFILENAME\"" \
    --header "Accept: application/json" \
    --header "X-Okapi-Tenant: $OKAPI_TENANT" \
    --header "X-Okapi-Token: $OKAPI_TOKEN" \
        > $tmpfile7
JOBPROFILEID=`jq -r -M '.jobProfiles[0].id' $tmpfile7`
JOBPROFILEDATATYPE=`jq -r -M '.jobProfiles[0].dataType' $tmpfile7`
JPNAME=`jq -r -M '.jobProfiles[0].name' $tmpfile7`
 echo "JOBPROFILEID=$JOBPROFILEID"
 echo "JOBPROFILEDATATYPE=$JOBPROFILEDATATYPE"
 echo "JPNAME=$JPNAME"
 echo

echo "=== 7. Launch import processing ==="
curl --silent --location --request POST "$OKAPI_URL/data-import/uploadDefinitions/$uploadDefinitionId/processFiles?defaultMapping=true" \
    --header "Content-Type: application/json" \
    --header "X-Okapi-Tenant: $OKAPI_TENANT" \
    --header "X-Okapi-Token: $OKAPI_TOKEN" \
    --data-raw "{
        \"uploadDefinition\": `cat $tmpfile6`,
        \"jobProfileInfo\": {
          \"id\": \"$JOBPROFILEID\",
          \"name\": \"$JPNAME\",
          \"dataType\": \"$JOBPROFILEDATATYPE\"
        }
          }"
        > $tmpfile8
