#!/usr/bin/env python3
"""Offline metadata execution regressions against the existing publisher guard.

Only in-memory workflow dictionaries are mutated; no payload is executed.
"""
import copy
import importlib.util
from pathlib import Path
import sys
import unittest

sys.dont_write_bytecode = True


def imported(filename):
    spec = importlib.util.spec_from_file_location(
        'metadata_' + filename.replace('-', '_'), Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PublisherMetadataControls(unittest.TestCase):
    def setUp(self):
        self.controls = imported('ci-build-publisher-tests.py')
        self.original = self.controls.SettledWorkflowControls().doc()

    def guard(self, document):
        self.controls.publisher_execution_guard(document)

    rejection = ValueError

    def test_actual_workflow_passes(self):
        self.guard(copy.deepcopy(self.original))

    def reject_metadata(self, publisher, key, value):
        document = copy.deepcopy(self.original)
        target = document['jobs']['publish'] if publisher else document
        target[key] = value
        with self.assertRaises(self.rejection,
                msg='actual publisher guard accepted payload execution metadata'):
            self.guard(document)

    def test_publisher_default_shell_rejected(self):
        self.reject_metadata(True, 'defaults', {
            'run': {'shell': 'bash delivery/payload.sh {0}'}})

    def test_workflow_default_shell_rejected(self):
        self.reject_metadata(False, 'defaults', {
            'run': {'shell': 'bash delivery/payload.sh {0}'}})

    def test_publisher_bash_env_rejected(self):
        environment = copy.deepcopy(self.original['jobs']['publish'].get('env', {}))
        environment['BASH_ENV'] = 'delivery/payload.sh'
        self.reject_metadata(True, 'env', environment)

    def test_workflow_bash_env_rejected(self):
        environment = copy.deepcopy(self.original.get('env', {}))
        environment['BASH_ENV'] = 'delivery/payload.sh'
        self.reject_metadata(False, 'env', environment)


if __name__ == '__main__':
    unittest.main(verbosity=2)
