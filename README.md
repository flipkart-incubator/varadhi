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

Run the following to start the below dependencies.

```docker compose -f setup/docker/compose.yml -p docker --profile dev up -d```

OR

Start [Dev, start pulsar and zk](.run%2FDev%2C%20start%20pulsar%20and%20zk.run.xml) IntelliJ run profile.

#### Message Broker

Varadhi needs a message broker for message persistence and delivery. [Apache Pulsar](https://pulsar.apache.org/) is used
as underlying message broker. For the development environment users can use containerised Pulsar in a standalone mode.
Details can be found [here](https://pulsar.apache.org/docs/3.0.x/standalone-docker/).

#### Persistent Store

For storing metadata about various Varadhi entities, a datastore is needed. [Zookeeper](https://zookeeper.apache.org/)
is used as global datastore to persist json formatted entities. For the development environment, containerised Zookeeper
can be
used in a standalone mode. Details can be found [here](https://hub.docker.com/_/zookeeper).

### Varadhi Server

To run the Varadhi server execute below from repo root.

```./gradlew run --args="src/main/resources/configuration.yml```

OR

Start [varadhi local \[run\]](.run%2Fvaradhi%20local%20%5Brun%5D.run.xml) IntelliJ run profile.

## k8s Deployment

```helm install varadhi setup/helm/varadhi -f setup/helm/varadhi/fcp.values.yaml```
```helm install varadhi-controller setup/helm/varadhi -f setup/helm/varadhi/values/controller_values.yaml```

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
Refer to [Contributing](./CONTRIBUTING.md).

You can also reachout to sahil.chachan@flipkart.com or k.dhruv@flipkart.com.

## Bugs and Feedback

[ TBD ]

## Blogs

[Effective Failure Handling in Flipkart’s Message Bus](https://blog.flipkart.tech/effective-failure-handling-in-flipkarts-message-bus-436c36be76cc)

## LICENSE

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
