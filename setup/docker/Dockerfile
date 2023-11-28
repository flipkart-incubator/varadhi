# base image for this should be jre based on instead of jdk.
# Ubuntu 22.04.2 LTS 
FROM eclipse-temurin:17.0.7_7-jdk
ARG PORT=8488
ARG VARADHI_HOME=.

# Varadhi Server
EXPOSE $PORT

# Install common debug tools
RUN apt-get update && apt-get install -y sudo  atop netcat screen procps iftop net-tools dstat jq iptables lsof iotop sysstat
RUN apt-get update && apt-get install -y tcpdump ngrep libcap2-bin
RUN setcap cap_net_raw,cap_net_admin=eip /usr/bin/tcpdump
RUN setcap cap_net_raw,cap_net_admin=eip /usr/bin/ngrep

# Install Zookeeper client.
RUN mkdir -p /tools/zk
WORKDIR /tools/zk
RUN wget https://archive.apache.org/dist/zookeeper/zookeeper-3.6.0/apache-zookeeper-3.6.0-bin.tar.gz
RUN tar -xf apache-zookeeper-3.6.0-bin.tar.gz
RUN rm apache-zookeeper-3.6.0-bin.tar.gz

# Install Pulsar Client.
RUN mkdir -p /tools/pulsar
WORKDIR /tools/pulsar
RUN wget https://archive.apache.org/dist/pulsar/pulsar-3.0.0/apache-pulsar-shell-3.0.0-bin.tar.gz
RUN tar -xf apache-pulsar-shell-3.0.0-bin.tar.gz
RUN rm apache-pulsar-shell-3.0.0-bin.tar.gz


# Install Varadhi i.e. copy server jar, its dependencies and configs.
RUN mkdir -p /usr/share/varadhi/dependencies && chmod -R 755 /usr/share/varadhi
RUN mkdir -p /etc/varadhi && chmod -R 755 /etc/varadhi
COPY $VARADHI_HOME/server/build/dependencies/ /usr/share/varadhi/dependencies/
COPY $VARADHI_HOME/pulsar/build/libs/pulsar-*.jar /usr/share/varadhi/dependencies/
COPY $VARADHI_HOME/pulsar/build/dependencies/ /usr/share/varadhi/dependencies/
COPY $VARADHI_HOME/server/build/libs/ /usr/share/varadhi/
COPY $VARADHI_HOME/server/build/resources/main/ /etc/varadhi/

# Fix configuration files references in the serverConfig.yml.
RUN sed -i 's|src/main/resources/|/etc/varadhi/|' /etc/varadhi/serverConfig.yml

RUN useradd varadhi --no-create-home --uid 10222 --shell /usr/sbin/nologin

USER varadhi
WORKDIR /usr/share/varadhi

# Start Varadhi server
CMD java -cp ./*:dependencies/* com.flipkart.varadhi.Server /etc/varadhi/serverConfig.yml
