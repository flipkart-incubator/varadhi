# base image for this should be jre based on instead of jdk.
# Ubuntu 24.04.1 LTS
FROM eclipse-temurin:21.0.5_11-jdk
ARG SKIP_CERT_CHECK=false
ARG VARADHI_HOME=.
ARG ZOOKEEPER_VERSION=3.6.0
ARG PULSAR_VERSION=3.0.0

ENV PORT=18488
ENV JVM_DEBUG_PORT=""

# Varadhi Server
EXPOSE $PORT

# Install common debug tools
RUN apt-get update && apt-get install -y dstat iptables
RUN apt-get install -y sudo atop screen procps iftop net-tools jq iptables lsof iotop sysstat
RUN apt-get install -y tcpdump ngrep libcap2-bin
RUN setcap cap_net_raw,cap_net_admin=eip /usr/bin/tcpdump
RUN setcap cap_net_raw,cap_net_admin=eip /usr/bin/ngrep

# Install Zookeeper client.
RUN mkdir -p /tools/zk
WORKDIR /tools/zk
RUN if "$SKIP_CERT_CHECK";  \
    then wget https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz --no-check-certificate ;  \
    else wget https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz ;  \
    fi
RUN tar -xf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
RUN rm apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz

# Install Pulsar Client.
RUN mkdir -p /tools/pulsar
WORKDIR /tools/pulsar
RUN if "$SKIP_CERT_CHECK";  \
    then wget https://archive.apache.org/dist/pulsar/pulsar-${PULSAR_VERSION}/apache-pulsar-shell-${PULSAR_VERSION}-bin.tar.gz --no-check-certificate ;  \
    else wget https://archive.apache.org/dist/pulsar/pulsar-${PULSAR_VERSION}/apache-pulsar-shell-${PULSAR_VERSION}-bin.tar.gz ;  \
    fi
RUN tar -xf apache-pulsar-shell-${PULSAR_VERSION}-bin.tar.gz
RUN rm apache-pulsar-shell-${PULSAR_VERSION}-bin.tar.gz

RUN useradd varadhi --no-create-home --uid 10222 --shell /usr/sbin/nologin

# Install Varadhi i.e. copy server jar, its dependencies and configs.
RUN mkdir -p /usr/share/varadhi/dependencies && chmod -R 777 /usr/share/varadhi
RUN mkdir -p /etc/varadhi && chmod -R 777 /etc/varadhi
COPY $VARADHI_HOME/server/build/dependencies/ /usr/share/varadhi/dependencies/
# pulsar module is not a compile time dependency for server, so we need to copy it separately.
COPY $VARADHI_HOME/pulsar/build/libs/pulsar-*.jar /usr/share/varadhi/dependencies/
COPY $VARADHI_HOME/pulsar/build/dependencies/ /usr/share/varadhi/dependencies/
# main application jar
COPY $VARADHI_HOME/server/build/libs/ /usr/share/varadhi/

# Install setup script
RUN mkdir -p /usr/share/varadhi/scripts && chmod -R 777 /usr/share/varadhi/scripts
COPY  --chmod=777 $VARADHI_HOME/setup/docker/scripts/* /usr/share/varadhi/scripts/

# Copy default configs. by default the directory is empty
COPY --chmod=777 $VARADHI_HOME/setup/docker/configs/varadhi-auto-generated/ /etc/varadhi/

USER varadhi
WORKDIR /usr/share/varadhi

# Start Varadhi server
CMD /usr/share/varadhi/scripts/entrypoint.sh
