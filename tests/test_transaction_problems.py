"""Resolve-problem handling against fake libdnf5 objects.

libdnf5's Transaction.get_problems() returns a GoalProblem bitmask (an int),
not a list; readable messages come from get_resolve_logs_as_strings().
Run from the repository root: python3 -m unittest discover -s tests
"""
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from appcenter import dnf_backend, privileged_helper

NO_PROBLEM, SOLVER_ERROR, NOT_FOUND = 0, 1, 2
PROTECTED = "Problem: The operation would result in removing the following protected packages: nobara-welcome"


class FakeSettings:
    def __init__(self):
        self.skip_unavailable = False

    def set_skip_unavailable(self, value):
        self.skip_unavailable = value


class FakeTransaction:
    def __init__(self, problems=NO_PROBLEM, logs=()):
        self.problems = problems
        self.logs = list(logs)
        self.calls = []

    def get_problems(self):
        return self.problems

    def get_resolve_logs_as_strings(self):
        return self.logs

    def get_transaction_problems(self):
        return []

    def get_transaction_packages(self):
        return []

    def download(self):
        self.calls.append("download")

    def run(self):
        self.calls.append("run")
        return "success"


class FakeGoal:
    """Like libdnf5, an unmatched spec sets NOT_FOUND unless skip_unavailable."""

    def __init__(self, transaction, unmatched=()):
        self.transaction = transaction
        self.unmatched = set(unmatched)
        self.upgrades = []

    def add_upgrade(self, spec, settings=None):
        self.upgrades.append((spec, settings))
        if spec in self.unmatched and not (settings and settings.skip_unavailable):
            self.transaction.problems |= NOT_FOUND

    def add_remove(self, spec):
        pass

    def add_install(self, spec):
        pass

    def resolve(self):
        return self.transaction


def fake_libdnf5(goal):
    return SimpleNamespace(base=SimpleNamespace(
        Goal=lambda base: goal,
        GoalJobSettings=FakeSettings,
        GoalProblem_NO_PROBLEM=NO_PROBLEM,
        Transaction=SimpleNamespace(TransactionRunResult_SUCCESS="success"),
    ))


def fake_backend(goal, installonly=()):
    backend = dnf_backend.DnfBackend.__new__(dnf_backend.DnfBackend)
    backend.libdnf5 = fake_libdnf5(goal)
    backend.base = SimpleNamespace(get_config=lambda: SimpleNamespace(installonlypkgs=list(installonly)))
    return backend


class PrivilegedHelperTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(privileged_helper, "emit")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_resolve_problem_is_reported_instead_of_crashing(self):
        transaction = FakeTransaction(SOLVER_ERROR, [PROTECTED])
        goal = FakeGoal(transaction)
        ok, message = privileged_helper._run_transaction(fake_libdnf5(goal), object(), "remove", "nobara-welcome")
        self.assertFalse(ok)
        self.assertEqual(message, PROTECTED)
        self.assertEqual(transaction.calls, [])

    def test_resolve_problem_without_logs_still_fails_cleanly(self):
        transaction = FakeTransaction(SOLVER_ERROR)
        ok, message = privileged_helper._run_transaction(fake_libdnf5(FakeGoal(transaction)), object(), "remove", "opengamepadui")
        self.assertFalse(ok)
        self.assertTrue(message)
        self.assertEqual(transaction.calls, [])

    def test_clean_resolve_runs_the_transaction(self):
        transaction = FakeTransaction()
        ok, _ = privileged_helper._run_transaction(fake_libdnf5(FakeGoal(transaction)), object(), "remove", "htop")
        self.assertTrue(ok)
        self.assertEqual(transaction.calls, ["download", "run"])


class BackendTransactionTests(unittest.TestCase):
    def test_root_mode_resolve_problem_is_reported_instead_of_crashing(self):
        transaction = FakeTransaction(SOLVER_ERROR, [PROTECTED])
        goal = FakeGoal(transaction)
        ok, message = fake_backend(goal)._run_transaction(goal, "Remove nobara-welcome")
        self.assertFalse(ok)
        self.assertEqual(message, PROTECTED)
        self.assertEqual(transaction.calls, [])

    def test_unmatched_installonly_names_do_not_hide_updates(self):
        # kernel-PAE and installonlypkg(vm) match nothing on a normal system.
        transaction = FakeTransaction()
        goal = FakeGoal(transaction, unmatched=["kernel-PAE", "installonlypkg(vm)"])
        backend = fake_backend(goal, installonly=["kernel", "kernel-PAE", "installonlypkg(vm)"])
        self.assertIs(backend._resolve_upgrade_transaction(), transaction)
        self.assertEqual(transaction.problems, NO_PROBLEM)

    def test_upgrade_check_with_solver_problem_returns_no_transaction(self):
        goal = FakeGoal(FakeTransaction(SOLVER_ERROR, ["nothing provides libfoo"]))
        self.assertIsNone(fake_backend(goal)._resolve_upgrade_transaction())


if __name__ == "__main__":
    unittest.main()
