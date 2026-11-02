# Spec Delta

## Purpose

Mask the short fence window of a topic failover from producers with a bounded server-side retry.

## ADDED Requirements

### Requirement: Retry produce while fenced
When the target topic is `Fenced` in the pod's region, the system MUST retry the produce up to 4 times (5 attempts including the initial one) with exponential backoff before returning `Fenced`.

#### Scenario: Fence clears within the buffer
- **WHEN** the topic is Fenced on the first attempt and Producing on a later attempt
- **THEN** the message is produced and the client receives success

#### Scenario: Fence outlasts the buffer
- **WHEN** the topic remains Fenced for all 4 retries (~5.37 s)
- **THEN** the client receives `ProduceStatus.Fenced` (HTTP 503) unchanged from today

### Requirement: Backoff schedule
The first retry MUST wait 1 s and each subsequent delay MUST be 1.2x the previous one, giving ~5.37 s total across 4 retries. Values are configurable.

#### Scenario: Default schedule
- **WHEN** the topic stays Fenced with default configuration
- **THEN** retries wait 1.000 / 1.200 / 1.440 / 1.728 s (added vs previous: +0.200 / +0.240 / +0.288 s; cumulative 1.000 / 2.200 / 3.640 / 5.368 s)

### Requirement: Re-evaluate topic state per attempt
Each attempt MUST re-read the topic from the cache and re-resolve the produce route.

#### Scenario: State changes mid-retry
- **WHEN** the topic becomes Blocked during retries
- **THEN** retrying stops and `NotAllowed` is returned

### Requirement: Only Fenced is retried
Throttled, Filtered, NotAllowed and storage Failed results MUST NOT be retried.

#### Scenario: Throttled
- **WHEN** a produce is throttled
- **THEN** it is returned immediately with no retry

### Requirement: Non-blocking retry
Backoff MUST NOT block the caller thread.

#### Scenario: Event loop
- **WHEN** a produce is waiting on backoff
- **THEN** no event-loop or worker thread is blocked
