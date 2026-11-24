from concurrent.futures import ThreadPoolExecutor
import sqlite3
import tempfile
import unittest
from pathlib import Path

from sqlite_outbox_lease import LeaseLost, SQLiteOutbox


class OutboxLeaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "outbox.sqlite3"
        self.outbox = SQLiteOutbox(self.path)

    def tearDown(self):
        self.temp.cleanup()

    def test_expired_lease_is_fenced_from_new_owner(self):
        message_id = self.outbox.enqueue("invoice.created", b"payload", now=10)
        first = self.outbox.claim("worker", now=10, lease_seconds=5)[0]
        self.assertEqual(first.id, message_id)
        self.assertEqual(first.attempts, 1)
        self.assertEqual(self.outbox.claim("worker", now=14, lease_seconds=5), [])

        second = self.outbox.claim("worker", now=15, lease_seconds=5)[0]
        self.assertEqual(second.attempts, 2)
        self.assertNotEqual(first.lease_token, second.lease_token)
        with self.assertRaises(LeaseLost):
            self.outbox.ack(message_id, first.lease_token, now=16)
        self.outbox.ack(message_id, second.lease_token, now=16)
        self.assertEqual(self.outbox.claim("next", now=17), [])

    def test_expired_lease_cannot_be_completed_before_reclaim(self):
        message_id = self.outbox.enqueue("invoice.created", b"payload", now=10)
        message = self.outbox.claim("worker", now=10, lease_seconds=5)[0]
        expired_at = 15
        operations = {
            "ack": lambda: self.outbox.ack(message_id, message.lease_token, now=expired_at),
            "retry": lambda: self.outbox.retry(
                message_id,
                message.lease_token,
                delay_seconds=0,
                error="late retry",
                now=expired_at,
            ),
            "extend": lambda: self.outbox.extend(
                message_id, message.lease_token, lease_seconds=5, now=expired_at
            ),
        }
        for operation_name, operation in operations.items():
            with self.subTest(operation=operation_name):
                with self.assertRaises(LeaseLost):
                    operation()

        reclaimed = self.outbox.claim("next-worker", now=expired_at, lease_seconds=5)
        self.assertEqual(len(reclaimed), 1)
        self.assertEqual(reclaimed[0].id, message_id)

    def test_extend_never_shortens_an_active_lease(self):
        message_id = self.outbox.enqueue("job", b"payload", now=10)
        message = self.outbox.claim("worker", now=10, lease_seconds=20)[0]

        self.outbox.extend(message_id, message.lease_token, now=11, lease_seconds=2)
        self.assertEqual(self.outbox.claim("other", now=13), [])

        self.outbox.extend(message_id, message.lease_token, now=14, lease_seconds=25)
        self.assertEqual(self.outbox.claim("other", now=30), [])
        reclaimed = self.outbox.claim("other", now=39)
        self.assertEqual([item.id for item in reclaimed], [message_id])

    def test_concurrent_workers_claim_disjoint_rows(self):
        for index in range(10):
            self.outbox.enqueue("job", str(index).encode(), now=1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            batches = list(pool.map(
                lambda worker: self.outbox.claim(worker, limit=5, now=2),
                ["worker-a", "worker-b"],
            ))
        claimed = [message.id for batch in batches for message in batch]
        self.assertEqual(len(claimed), 10)
        self.assertEqual(len(set(claimed)), 10)

    def test_retry_respects_available_at(self):
        message_id = self.outbox.enqueue("job", b"payload", now=10)
        message = self.outbox.claim("worker", now=10)[0]
        self.outbox.retry(message_id, message.lease_token, delay_seconds=5, error="busy", now=10)
        self.assertEqual(self.outbox.claim("worker-2", now=14), [])
        retried = self.outbox.claim("worker-2", now=15)[0]
        self.assertEqual(retried.attempts, 2)

    def test_enqueue_can_share_domain_transaction(self):
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("BEGIN")
            self.outbox.enqueue("rolled.back", b"payload", now=1, connection=connection)
            connection.rollback()
        finally:
            connection.close()
        self.assertEqual(self.outbox.claim("worker", now=2), [])

    def test_non_finite_time_and_lease_cannot_strand_message(self):
        with self.assertRaises(ValueError):
            self.outbox.enqueue("job", b"payload", now=float("inf"))
        message_id = self.outbox.enqueue("job", b"payload", now=10)

        for duration in (float("nan"), float("inf")):
            with self.subTest(duration=duration):
                with self.assertRaises(ValueError):
                    self.outbox.claim("worker", now=10, lease_seconds=duration)
        with self.assertRaises(ValueError):
            self.outbox.claim("worker", now=1e308, lease_seconds=1e308)

        claimed = self.outbox.claim("worker", now=10, lease_seconds=5)
        self.assertEqual([message.id for message in claimed], [message_id])

    def test_non_finite_retry_and_extension_leave_lease_reclaimable(self):
        message_id = self.outbox.enqueue("job", b"payload", now=10)
        message = self.outbox.claim("worker", now=10, lease_seconds=5)[0]

        with self.assertRaises(ValueError):
            self.outbox.retry(
                message_id, message.lease_token,
                delay_seconds=float("inf"), error="busy", now=11,
            )
        with self.assertRaises(ValueError):
            self.outbox.retry(
                message_id, message.lease_token,
                delay_seconds=1e308, error="busy", now=1e308,
            )
        with self.assertRaises(ValueError):
            self.outbox.extend(
                message_id, message.lease_token,
                lease_seconds=float("nan"), now=11,
            )
        with self.assertRaises(ValueError):
            self.outbox.ack(message_id, message.lease_token, now=float("nan"))

        reclaimed = self.outbox.claim("next-worker", now=15)
        self.assertEqual([item.id for item in reclaimed], [message_id])


if __name__ == "__main__":
    unittest.main()
