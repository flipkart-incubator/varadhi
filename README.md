# Varadhi : RESTBus - A message bus implementation with a REST interface

[![build](https://github.com/flipkart-incubator/varadhi/actions/workflows/gradlebuild.yml/badge.svg?branch=master)](https://github.com/flipkart-incubator/varadhi/actions/workflows/gradlebuild.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Introduction

Varadhi is the open source version of a similar FK internal project, a message bus implementation with a REST interface.
Varadhi supports both P2P (point-to-point) and Publish-Subscribe messaging models. The idea behind RESTBus is to take
your HTTP API stack and automatically transform them into service bus driven, queue/pub-sub, message oriented endpoints.
The communication between Varadhi and endpoints will be on HTTP.

Varadhi is in active development and has been in production since last 10 years inside Flipkart as the backbone of async
REST communication between various microservices sending billions of messages daily.

With Varadhi we hope others would be able to benefit from our learnings of a decade.

## Full Documentation

See the [Wiki](https://github.com/flipkart-incubator/varadhi/wiki/) for concepts, use cases, architecture details, API
spec and other detailed information.

## Build

./gradlew build

## Run

### Dependencies

To provide the required functionality Varadhi takes a dependencies on various tech stack.
Current development environment supports below option for these.

#### Message Broker

Varadhi needs a message broker for message persistence and delivery. [Apache Pulsar](https://pulsar.apache.org/) is used
as underlying message broker. For the development environment users can use containerised Pulsar in a standalone mode.
Details can be found [here](https://pulsar.apache.org/docs/3.0.x/standalone-docker/).

```docker run -it -p 6650:6650 -p 8081:8080 --mount source=pulsardata,target=/pulsar/data --mount source=pulsarconf,target=/pulsar/conf apachepulsar/pulsar:3.0.0 bin/pulsar standalone```

#### Persistent Store

For storing metadata about various Varadhi entities, a datastore is needed. [Zookeeper](https://zookeeper.apache.org/)
is used as global datastore to persist json formatted entities. For the development environment, containerised Zookeeper
can be
used in a standalone mode. Details can be found [here](https://hub.docker.com/_/zookeeper).

```docker run --name some-zookeeper --restart always -d -p  2181:2181 -p 2888:2888 -p 3888:3888 -p 8082:8080 zookeeper```

### Varadhi Server

To run the Varadhi server execute below from repo root.

```./gradlew run --args="src/main/resources/configuration.yml"```

## Modules

- entities: It has all the entities used by the spi module and varadhi apis.
- spi: It has all the interfaces related to persistence and messaging.
- pulsar: It contains messaging-spi implementation using Apache Pulsar.
- core: It contains all the core logic of `Varadhi` and is relied upon by various other sub-modules.
- server: It is the entry point of `Varadhi`. It houses all server related logic and binds together all other modules.

## Releases

[ TBD ]

## Roadmap

**Status:** Initial Review Completed.

See the Wiki page [Roadmap](https://github.com/flipkart-incubator/varadhi/wiki/Roadmap) for details.

## Communications

[ TBD ]

## Want to Contribute ?

Reachout to sahil.chachan@flipkart.com or k.dhruv@flipkart.com incase you want to contribute here :)

## Bugs and Feedback

[ TBD ]

## Blogs

[Effective Failure Handling in Flipkart’s Message Bus](https://blog.flipkart.tech/effective-failure-handling-in-flipkarts-message-bus-436c36be76cc)

## LICENSE

[ TBD ]
