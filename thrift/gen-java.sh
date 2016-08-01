

WORK_DIR=$(cd `dirname "$0"`/.. ; pwd)

rm -rf ${WORK_DIR}/pike/src/main/java/com/pplive/pike/thriftgen

${WORK_DIR}/tools/linux/thrift-0.9.0 -out ${WORK_DIR}/pike/src/main/java -r --gen java ${WORK_DIR}/thrift/metadata.thrift

${WORK_DIR}/tools/linux/thrift-0.9.0 -out ${WORK_DIR}/pike/src/main/java -r --gen java ${WORK_DIR}/thrift/datetransfer.thrift