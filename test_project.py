import json
import os
import unittest
from pathlib import Path

class FixtureContracts(unittest.TestCase):

    def test_cdc_fixture_exercises_updates_deletes_and_rejections(self):
        rows = [json.loads(x) for x in Path('sample_cdc.jsonl').read_text().splitlines()]
        self.assertEqual({r['op'] for r in rows}, {'I', 'U', 'D'})
        self.assertEqual(sum((r['customer_id'] is None for r in rows)), 1)

@unittest.skipUnless(os.environ.get('RUN_SPARK_TESTS') == '1', 'Set RUN_SPARK_TESTS=1 with Java and Spark installed')
class SparkIntegration(unittest.TestCase):

    def test_delta_cdc_replay_and_stale_delete(self):
        import tempfile
        from pipeline import run
        with tempfile.TemporaryDirectory() as tmp:
            config = json.loads(Path('config.json').read_text())
            config['root'] = f'{tmp}/lake'
            first = run(config)
            self.assertEqual(first['active_customers'], 2)
            self.assertEqual(first['quarantined_rows'], 1)
            self.assertEqual(run(config), first)
            stale = Path(tmp, 'stale.jsonl')
            stale.write_text(json.dumps({'customer_id': 'C02', 'sequence': 1, 'op': 'I', 'name': 'Morgan', 'region': 'West'}))
            config['source'] = str(stale)
            self.assertEqual(run(config)['active_customers'], 2)
if __name__ == '__main__':
    unittest.main()
