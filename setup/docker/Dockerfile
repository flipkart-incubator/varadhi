# base image for this should be jre based on instead of jdk.
# Ubuntu 22.04.2 LTS 
FROM eclipse-temurin:17.0.7_7-jdk

ARG VARADHI_HOME=.
# set the env to "test" for using this image as part of compose setup
# this will setup the correct ip and port in pulsar and zk config files on Varadhi server.
ARG ENV="dev"

# Varadhi Server
EXPOSE 8080

# Install Varadhi,  configs and dependencies etc.
RUN mkdir -p /usr/share/varadhi/dependencies && chmod 755 /usr/share/varadhi/dependencies
RUN mkdir /etc/varadhi && chmod 755 /etc/varadhi
COPY $VARADHI_HOME/server/build/dependencies/ /usr/share/varadhi/dependencies/
COPY $VARADHI_HOME/pulsar/build/libs/pulsar-*.jar /usr/share/varadhi/dependencies/
COPY $VARADHI_HOME/pulsar/build/dependencies/ /usr/share/varadhi/dependencies/
COPY $VARADHI_HOME/server/build/libs/ /usr/share/varadhi/
COPY $VARADHI_HOME/server/build/resources/main/ /etc/varadhi/

# Fix configuration files for dev setup, this should move out of docker build
RUN sed -i 's|".*pulsarConfig.yml"|"/etc/varadhi/pulsarConfig.yml"|' /etc/varadhi/configuration.yml
RUN sed -i 's|".*zkConfig.yml"|"/etc/varadhi/zkConfig.yml"|' /etc/varadhi/configuration.yml
RUN sed -i 's|".*authorizationConfig.yml"|"/etc/varadhi/authorizationConfig.yml"|' /etc/varadhi/configuration.yml
RUN sed -i 's|".*zkConfig.yml"|"/etc/varadhi/zkConfig.yml"|' /etc/varadhi/authorizationConfig.yml

# ensure test mechanism is enabled for authn
RUN  if [ "$ENV" = "test" ] ; then sed -i  "/authentication:/ {N; s|authentication:\n  .*|authentication:\n  user_header:|}" /etc/varadhi/configuration.yml ; fi

RUN  if [ "$ENV" = "test" ] ; then sed -i  "s|127.0.0.1:8081|10.5.5.3:8080|" /etc/varadhi/pulsarConfig.yml ; fi
RUN  if [ "$ENV" = "test" ] ; then sed -i  "s|127.0.0.1|10.5.5.4|" /etc/varadhi/zkConfig.yml ; fi
RUN  if [ "$ENV" = "test" ] ; then sed -i  "s|127.0.0.1|10.5.5.4|" /etc/varadhi/configuration.yml ; fi


RUN useradd varadhi

USER varadhi
WORKDIR /usr/share/varadhi

# Start Varadhi server
CMD java -cp ./*:dependencies/* -Dcom.sun.management.jmxremote.port=9990 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false com.flipkart.varadhi.Server /etc/varadhi/configuration.yml
