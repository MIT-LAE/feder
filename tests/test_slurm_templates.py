"""Deployment templates are valid shell without a Slurm installation."""

from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / 'deploy'


@pytest.mark.skipif(shutil.which('bash') is None, reason='bash is unavailable')
@pytest.mark.parametrize('template', [
    'feder-rx-scheduled.sbatch.template',
    'feder-ingest-ready-queue.sbatch.template',
    'submit-feder-rx-scheduled.template',
    'submit-feder-ingest-ready-queue.template',
])
def test_slurm_shell_templates_pass_bash_syntax_check(template: str) -> None:
    subprocess.run(['bash', '-n', DEPLOY / template], check=True)


def test_receiver_submission_template_uses_squeue_and_singleton() -> None:
    contents = (DEPLOY / 'submit-feder-rx-scheduled.template').read_text()

    assert 'JOB_NAME="feder-rx-scheduled"' in contents
    assert 'squeue' in contents
    assert '--dependency=singleton' in contents
    assert '0 */6 * * *' in contents


def test_ingester_submission_template_uses_squeue_and_singleton() -> None:
    contents = (DEPLOY / 'submit-feder-ingest-ready-queue.template').read_text()

    assert 'JOB_NAME="feder-ingest-ready-queue"' in contents
    assert 'squeue' in contents
    assert '--dependency=singleton' in contents
    assert 'daily' in contents


@pytest.mark.parametrize('template, command', [
    ('feder-rx-scheduled.sbatch.template', 'feder-rx-scheduled'),
    ('feder-ingest-ready-queue.sbatch.template', 'feder-ingest'),
])
def test_sbatch_templates_expose_required_site_configuration(
        template: str, command: str
) -> None:
    contents = (DEPLOY / template).read_text()

    for directive in (
        '--account=REPLACE_WITH_ACCOUNT',
        '--partition=REPLACE_WITH_PARTITION',
        '--time=REPLACE_WITH_TIME_LIMIT',
        '--output=REPLACE_WITH_LOG_DIRECTORY',
        '--error=REPLACE_WITH_LOG_DIRECTORY',
        '--mail-type=FAIL',
        '--mail-user=REPLACE_WITH_FAILURE_EMAIL',
    ):
        assert directive in contents
    assert 'FEDER_REPOSITORY="REPLACE_WITH_FEDER_REPOSITORY"' in contents
    assert 'FEDER_CONFIG="REPLACE_WITH_CONFIG_PATH"' in contents
    assert command in contents


def test_slurm_operator_documentation_describes_queue_recovery_contract() -> None:
    contents = (DEPLOY / 'README-slurm.md').read_text()

    for text in (
        'cursor.json',
        'incomplete/',
        'ready/',
        'one chunk per six-hourly job',
        'duplicate-safe',
        'half-open UTC range',
        'does **not** read or modify the scheduled\n`cursor.json`',
        'shared-filesystem file\nlocks',
    ):
        assert text in contents
