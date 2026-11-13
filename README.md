# SQLite Outbox Lease

> **Status:** Early-stage reference implementation; APIs may change.

A small SQLite transactional outbox for background work that must survive process restarts.

The library stores messages durably, lets workers claim them with expiring leases, and uses a unique lease token to reject acknowledgements from stale workers. It is useful when a service already uses SQLite and needs a clear, testable foundation for reliable dispatch.

## Example

```python
from sqlite_outbox_lease import SQLiteOutbox

outbox = SQLiteOutbox("service.sqlite3")
outbox.enqueue("email.send", b'{"to":"dev@example.com"}')

for message in outbox.claim("worker-1", limit=10, lease_seconds=30):
    try:
        send_to_provider(message.topic, message.payload)
    except TemporaryProviderError as error:
        outbox.retry(message.id, message.lease_token, error=str(error), delay_seconds=10)
    else:
        outbox.ack(message.id, message.lease_token)
```

For atomic writes, pass the application's SQLite connection to `enqueue` inside the same transaction that updates domain data. A worker can reclaim a message after its lease expires. If an older worker later tries to acknowledge or retry it, the lease token no longer matches and `LeaseLost` is raised.

## What it provides

- Durable messages stored in SQLite.
- Ordered claims with bounded batch sizes.
- Expiring leases for worker recovery.
- Fencing tokens that prevent stale workers from completing a newer claim.
- Retry scheduling with a caller-supplied delay.
- An `ack` API for marking successful deliveries.

## Delivery model and limits

This is an **at-least-once** outbox primitive. A process can crash after a provider accepts a message but before `ack` commits, so consumers should be idempotent. The package does not provide a broker, automatic retry policy, payload codec, or schema migration framework. It uses SQLite WAL mode and file-backed databases; all workers must access the same database file.

## Development

```sh
python -m pip install -e .
python -m unittest discover -s tests -v
```

The GitHub Actions workflow runs the test suite on pushes and pull requests.

## License

MIT. See [LICENSE](LICENSE).
