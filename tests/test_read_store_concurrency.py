"""
test_read_store_concurrency.py — BL-18c.

The web server serves requests on multiple waitress threads. Sharing ONE BlockStore
connection across them caused intermittent SQLITE_MISUSE ("bad parameter or other API
misuse") under concurrent reads. A per-thread read-only store (own connection) is safe
alongside the engine's writer under WAL. This reproduces heavy concurrent reads + a
writer and asserts no error.
"""
import os, sys, tempfile, threading, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore, open_block_store


class TestReadStoreConcurrency(unittest.TestCase):
    def test_per_thread_read_no_misuse(self):
        d = tempfile.mkdtemp(); db = os.path.join(d, "blocks.db")
        rw = open_block_store(db)
        rw._conn.execute("INSERT OR IGNORE INTO config_periods "
                         "(id,effective_from,billing_day,block_minutes,timezone) "
                         "VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")
        rw._conn.execute(
            "INSERT INTO blocks (block_start,block_end,meter_id,config_period_id,"
            "imp_kwh,imp_rate,needs_review,review_reason) VALUES "
            "('2026-08-14T19:00:00','2026-08-14T19:30:00','electricity_main',1,3.0,0.32,"
            "1,'possible billing error')")
        rw._conn.commit()
        errors = []; stop = threading.Event()

        def writer():
            c = 0
            while not stop.is_set():
                try:
                    rw._conn.execute("UPDATE blocks SET imp_cost=? "
                                     "WHERE block_start='2026-08-14T19:00:00'", (c % 100 / 100.0,))
                    rw._conn.commit(); c += 1
                except Exception as e:
                    errors.append(("writer", repr(e)))

        def reader(tid):
            rs = BlockStore(db, read_only=True)   # per-thread read connection (the fix)
            for _ in range(300):
                try:
                    self.assertGreaterEqual(len(rs.get_review_blocks()), 1)
                except Exception as e:
                    errors.append(("reader%d" % tid, repr(e)))

        wt = threading.Thread(target=writer); wt.start()
        rts = [threading.Thread(target=reader, args=(i,)) for i in range(6)]
        [t.start() for t in rts]; [t.join() for t in rts]
        stop.set(); wt.join()
        self.assertEqual(errors, [], "SQLITE_MISUSE / concurrency error under load")


if __name__ == "__main__":
    unittest.main()
