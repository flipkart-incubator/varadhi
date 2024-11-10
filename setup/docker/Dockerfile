# base image for this should be jre based on instead of jdk.
# Ubuntu 24.04.1 LTS
FROM eclipse-temurin:21.0.5_11-jdk
ARG SKIP_CERT_CHECK=true
ARG VARADHI_HOME=.

ENV PORT=18488
ENV PULSAR_URL=
ENV ZK_URL=

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
    then wget https://archive.apache.org/dist/zookeeper/zookeeper-3.6.0/apache-zookeeper-3.6.0-bin.tar.gz --no-check-certificate ;  \
    else wget https://archive.apache.org/dist/zookeeper/zookeeper-3.6.0/apache-zookeeper-3.6.0-bin.tar.gz ;  \
    fi
RUN tar -xf apache-zookeeper-3.6.0-bin.tar.gz
RUN rm apache-zookeeper-3.6.0-bin.tar.gz

# Install Pulsar Client.
RUN mkdir -p /tools/pulsar
WORKDIR /tools/pulsar
RUN if "$SKIP_CERT_CHECK";  \
    then wget https://archive.apache.org/dist/pulsar/pulsar-3.0.0/apache-pulsar-shell-3.0.0-bin.tar.gz --no-check-certificate ;  \
    else wget https://archive.apache.org/dist/pulsar/pulsar-3.0.0/apache-pulsar-shell-3.0.0-bin.tar.gz ;  \
    fi
RUN tar -xf apache-pulsar-shell-3.0.0-bin.tar.gz
RUN rm apache-pulsar-shell-3.0.0-bin.tar.gz


RUN useradd varadhi --no-create-home --uid 10222 --shell /usr/sbin/nologin

# Install Varadhi i.e. copy server jar, its dependencies and configs.
RUN mkdir -p /usr/share/varadhi/dependencies && chmod -R 777 /usr/share/varadhi
RUN mkdir -p /etc/varadhi && chmod -R 777 /etc/varadhi
COPY $VARADHI_HOME/server/build/dependencies/ /usr/share/varadhi/dependencies/
COPY $VARADHI_HOME/pulsar/build/libs/pulsar-*.jar /usr/share/varadhi/dependencies/
COPY $VARADHI_HOME/pulsar/build/dependencies/ /usr/share/varadhi/dependencies/
COPY $VARADHI_HOME/server/build/libs/ /usr/share/varadhi/
COPY --chmod=777 $VARADHI_HOME/server/build/resources/main/ /etc/varadhi/

# Install setup script
RUN mkdir -p /usr/share/varadhi/scripts && chmod -R 777 /usr/share/varadhi/scripts
COPY  --chmod=777 $VARADHI_HOME/setup/docker/scripts/* /usr/share/varadhi/scripts/

# Fix configuration files for dev setup, this should move out of docker build
RUN sed -i 's|".*pulsarConfig.yml"|"/etc/varadhi/pulsarConfig.yml"|' /etc/varadhi/configuration.yml
RUN sed -i 's|".*zkConfig.yml"|"/etc/varadhi/zkConfig.yml"|' /etc/varadhi/configuration.yml
RUN sed -i 's|".*authorizationConfig.yml"|"/etc/varadhi/authorizationConfig.yml"|' /etc/varadhi/configuration.yml
RUN sed -i 's|".*zkConfig.yml"|"/etc/varadhi/zkConfig.yml"|' /etc/varadhi/authorizationConfig.yml

# ensure test mechanism is enabled for authn
RUN  if [ "$ENV" = "test" ] ; then sed -i  "/authentication:/ {N; s|authentication:\n  .*|authentication:\n  user_header:|}" /etc/varadhi/configuration.yml ; fi
RUN  if [ "$ENV" = "test" ] ; then sed -i  "s|authenticationEnabled.*|authenticationEnabled: true|" /etc/varadhi/configuration.yml ; fi

RUN  if [ "$ENV" = "test" ] ; then sed -i  "s|127.0.0.1:8081|10.5.5.3:8080|" /etc/varadhi/pulsarConfig.yml ; fi
RUN  if [ "$ENV" = "test" ] ; then sed -i  "s|127.0.0.1|10.5.5.4|" /etc/varadhi/zkConfig.yml ; fi
RUN  if [ "$ENV" = "test" ] ; then sed -i  "s|127.0.0.1|10.5.5.4|" /etc/varadhi/configuration.yml ; fi

# Fix configuration files references in the configuration.yml.
RUN sed -i 's|src/main/resources/|/etc/varadhi/|' /etc/varadhi/configuration.yml

USER varadhi
WORKDIR /usr/share/varadhi

# Start Varadhi server
CMD /usr/share/varadhi/scripts/entrypoint.sh
