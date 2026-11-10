# Lifecycles

How publishes, consumers, messages and watchers behave over time, and how they recover.

## Overview

- **Publishes** are registered on a shared acknowledgement subscription of the context. An
  acknowledgement received before `AckFuture.wait()` is kept until it is awaited. When the number
  of unacknowledged publishes reaches the maximum, a publish waits for room for the stall timeout
  and then fails.
- **Fetches** own a reply subscription which is removed when the batch completes, expires, fails,
  or the iteration ends early.
- **Continuous consumption** keeps one reply subscription for its lifetime. Messages are buffered up
  to a limit; buffered messages are released when they are iterated over, which triggers new pull
  requests. Reconnects, missed heartbeats, leadership changes and pin mismatches reset the buffer
  accounting and send new pull requests. Deleted consumers, push consumers and invalid requests end
  the iteration with an error. `stop()` ends it immediately, `drain()` after the buffered messages.
- **Ordered consumers** track the stream and consumer sequences of delivered messages. On a gap, a
  lost consumer or missed heartbeats, the underlying consumer is deleted and recreated from the next
  stream sequence, with exponential backoff.
- **Push consumers** allow one active consumption per instance and reply to flow control requests.
  The server stops sending messages and heartbeats to deleted push consumers, which is reported as
  missed heartbeats.
- **Messages** can be acknowledged once; in progress acknowledgements can be repeated until then.
- **Watchers** (Key-Value Store and Object Store) deliver the current state, a `nil` marker, then
  live updates, and are built on ordered consumers so they recover the same way.
