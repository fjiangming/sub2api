import importlib.util
from pathlib import Path
import unittest


spec = importlib.util.spec_from_file_location(
    'require_fork_checks', Path(__file__).with_name('require_fork_checks.py'))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
SHA = 'a' * 40


def run(workflow='backend-ci.yml', **changes):
    return {'id': 1, 'head_sha': SHA, 'head_branch': 'dev', 'event': 'push',
            'path': f'.github/workflows/{workflow}', 'status': 'completed',
            'conclusion': 'success', **changes}


class ForkReleaseGateTest(unittest.TestCase):
    def test_only_exact_commit_branch_event_and_workflow_count(self):
        for changes in ({'head_sha': 'b' * 40}, {'head_branch': 'main'},
                        {'event': 'pull_request'}, {'path': '.github/workflows/other.yml'}):
            with self.subTest(changes=changes):
                self.assertEqual(gate.workflow_status([run(**changes)], 'backend-ci.yml', SHA), 'missing')

    def test_failed_cancelled_or_skipped_checks_block_publication(self):
        for conclusion in ('failure', 'cancelled', 'skipped', 'timed_out', None):
            with self.subTest(conclusion=conclusion), self.assertRaises(RuntimeError):
                gate.workflow_status([run(conclusion=conclusion)], 'backend-ci.yml', SHA)

    def test_newer_failed_run_cannot_be_hidden_by_older_success(self):
        with self.assertRaises(RuntimeError):
            gate.workflow_status([run(), run(id=2, conclusion='failure')], 'backend-ci.yml', SHA)

    def test_in_progress_rerun_does_not_count_as_completed_success(self):
        self.assertEqual(gate.workflow_status(
            [run(status='in_progress', conclusion=None)], 'backend-ci.yml', SHA), 'in_progress')

    def test_both_workflows_must_finish_before_publication(self):
        pending = [True]
        calls = []

        def fetch(workflow):
            calls.append(workflow)
            return [run(workflow, status='in_progress' if pending[0] else 'completed')]

        gate.wait_for_checks(fetch, SHA, sleep=lambda _: pending.__setitem__(0, False))
        self.assertEqual(calls, list(gate.WORKFLOWS) * 2)

    def test_missing_checks_eventually_block_publication(self):
        ticks = iter((0, 0, 2))
        with self.assertRaisesRegex(RuntimeError, 'Timed out'):
            gate.wait_for_checks(lambda _: [], SHA, timeout=1,
                                 clock=lambda: next(ticks), sleep=lambda _: None)

    def test_api_failure_is_not_treated_as_success(self):
        def unavailable(_):
            raise OSError('API unavailable')

        with self.assertRaises(OSError):
            gate.wait_for_checks(unavailable, SHA)


if __name__ == '__main__':
    unittest.main()
