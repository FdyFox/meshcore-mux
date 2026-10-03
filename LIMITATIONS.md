# Limitations

MeshCore's companion protocol was designed around one client connected to one companion. `meshcore-mux` coordinates multiple clients, but it cannot remove every protocol or shared-state constraint.

## Outgoing messages are not mirrored

If one client sends a DM or channel message, other clients cannot see it as an outgoing message because the companion protocol has no outgoing-message event. Incoming messages are copied to all clients. DM delivery confirmations are broadcast, but they do not contain the original message. Fixing this completely requires a firmware event containing an outgoing message's ID, content, and delivery state.

## Transactions can briefly delay other clients

Some operations need a round trip to the companion and can delay other clients for a few seconds, for example listing contacts, querying or changing settings, or waiting for a send acknowledgement. Delayed clients continue when the transaction completes.

Only one login or other remote request can be pending at a time. Concurrent attempts are rejected with `BAD_STATE` and logged as a warning, and repeater login state is shared. While a remote request is pending, another client cannot start status, telemetry, binary, path-discovery, anonymous, or trace requests; local queries, incoming messages, and outgoing messages continue.

## Error details reach only the log

Clients receive only the native `ERR` reason byte (for example `BAD_STATE`), because the protocol cannot carry an error text. The mux log records the actual cause, such as which resource was busy.

## Identity and radio configuration are shared

Every client uses the companion's mesh identity, contacts, channels, and radio settings. A configuration change by one application affects all of them, and others may not notice until they reconnect. Use one application as the configuration authority when clients have competing policies.

## Inbox retention is bounded and volatile

Multi-client sessions are connection-scoped; a new client does not receive messages distributed before it connected. Dedicated ports provide a stable identity and a bounded reconnect queue. By default the queue lives in RAM and does not survive a restart of the mux. With persistence enabled it survives graceful restarts completely, but a crash can lose or repeat the changes of the last flush interval. In both cases, delivery does not prove that the application processed an item after it was written to the socket. See [CONFIGURATION.md](CONFIGURATION.md#dedicated-queues).

## The mux must be the companion's only client

All participating applications must connect through the mux. Connecting to the same companion directly over TCP, BLE, or USB bypasses its coordination and produces incorrect client state.

## The downstream network is trusted

Listener ports have no authentication or encryption. Anyone who can reach a port can use the shared companion; anyone who reaches a dedicated port can replace its current socket and consume its queue. This matches the companion's own TCP interface. Bind listeners only to appropriate interfaces and protect them with a firewall or VPN.

## State of this port

This Go port reproduces the original's behavior and has been tested with unit tests and a simulated companion over real TCP. It has not yet been verified against physical companion hardware or the real-client smoke tests (`meshcore_py`, `meshcore-cli`) of the original.
