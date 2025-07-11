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

## Try locally

Give it a go locally on your machine. You will require java 21, docker & python.
Follow this guide: [Wiki/Try Locally](https://github.com/flipkart-incubator/varadhi/wiki/Try-Locally)

## Build

```bash
./gradlew build test
```

## Integration Tests

```bash
./gradlew copyDependencies copyE2EConfig -x test

docker build . --file setup/docker/Dockerfile --tag varadhi.docker.registry/varadhi:latest --build-arg

docker compose --profile test -f setup/docker/compose.yml up -d --wait --wait-timeout 180

./gradlew testE2E
```

### Dependencies

To provide the required functionality, Varadhi takes dependency on various tech stack.

Run the following to start the below dependencies.
```docker compose --profile dev -f setup/docker/compose.yml up -d --wait --wait-timeout 180```

OR

#### Message Broker

Varadhi needs a message broker for message persistence and delivery. [Apache Pulsar](https://pulsar.apache.org/) is used
as underlying message broker. For the development environment users can use containerised Pulsar in a standalone mode.
Details can be found [here](https://pulsar.apache.org/docs/3.0.x/standalone-docker/).

#### Metadata Store

For storing metadata about various Varadhi entities, a datastore is needed. [Zookeeper](https://zookeeper.apache.org/)
is used as global datastore to persist json formatted entities. For the development environment, containerised Zookeeper
can be used in a standalone mode. Details can be found [here](https://hub.docker.com/_/zookeeper).

### Varadhi Server

Finally, to run the Varadhi server, provide the custom zk & pulsar endpoints at
`server/src/main/resources/config.overrides` and then do
```./gradlew run```

If you are using the dev profile in our docker compose to start the zk and pulsar, then simply do `./gradlew run`. No
config overrides are required.

## k8s Deployment

```bash
cd setup/helm/varadhi

helm install varadhi-server . -f values/common.values.yaml -f values/local.server.values.yaml

helm install varadhi-controller . -f values/common.values.yaml -f values/local.controller.values.yaml
```

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

- [Effective Failure Handling in Flipkart’s Message Bus](https://blog.flipkart.tech/effective-failure-handling-in-flipkarts-message-bus-436c36be76cc)
- [Flipkart to Community: Open Source Varadhi](https://www.youtube.com/watch?v=A9ET2lw6nxM&t=144s)

## LICENSE

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
