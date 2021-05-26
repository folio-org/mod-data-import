FROM folioci/alpine-jre-openjdk11:latest

ENV VERTICLE_FILE mod-data-import-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

# Copy your fat jar to the container
COPY target/${VERTICLE_FILE} ${VERTICLE_HOME}/${VERTICLE_FILE}

# Copy local lib to the container
ENV LIB_DIR ${VERTICLE_HOME}/lib
RUN mkdir -p ${LIB_DIR}
COPY lib/* ${LIB_DIR}/

# Expose this port locally in the container.
EXPOSE 8081
