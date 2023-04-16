# Varadhi : RESTBus - A message bus implementation with a REST interface

# Introduction

Varadhi is the open source version of a similar FK internal project, a message bus implementation with a REST interface.
Varadhi supports both P2P (point-to-point) and Publish-Subscribe messaging models. The idea behind RESTBus is to take
your HTTP API stack and automatically transform them into service bus driven, queue/pub-sub, message oriented endpoints.
The communication between Varadhi and endpoints will be on HTTP.

Varadhi is in active development and has been in production since last 10 years inside Flipkart as the backbone of async
REST communication between various microservices sending billions of messages daily.

With Varadhi we hope others would be able to benefit from our learnings of a decade.

## Full Documentation

See the [Wiki](wiki/) for concepts, use cases, architecture details, API spec and other detailed information.

## Build

./gradlew build

## Run

./gradlew run --args="src/main/resources/configuration.yml"

## Releases

[ TBD ]

## Roadmap

[ TBD ]

## Communications

[ TBD ]

## Want to Contribute ?

[ TBD ]

## Bugs and Feedback

[ TBD ]

## Blogs

[Effective Failure Handling in Flipkart’s Message Bus](https://blog.flipkart.tech/effective-failure-handling-in-flipkarts-message-bus-436c36be76cc)

## LICENSE

[ TBD ]
