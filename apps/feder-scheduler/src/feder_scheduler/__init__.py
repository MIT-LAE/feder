from datetime import date, datetime, time, timedelta
import logging
import os
import subprocess
import tomllib

import click

from feder_server import logging_setup


__version__ = '1.2.1'


logger = logging.getLogger(__name__)


def find_next(schedule_data: dict) -> tuple[datetime, list[str]]:
    today = date.today()
    now = datetime.now()
    next_jobs = []
    next_time = None

    # Check today and tomorrow.
    for day_offset in range(2):
        # Each job can have multiple times.
        for job, job_times in schedule_data.items():
            for job_time in job_times:
                # Make the job time into a datetime object for comparison.
                hour, minute = map(int, job_time.split(':'))
                actual_job_time = datetime.combine(
                    today + timedelta(days=day_offset), time(hour, minute)
                )

                # Skip jobs in the past.
                if actual_job_time <= now:
                    continue

                # Multiple jobs at the same time? Keep track of all of them.
                if next_time is not None and actual_job_time == next_time:
                    next_jobs.append(job)
                    continue

                # First valid job or a new earliest job.
                if next_time is None or actual_job_time < next_time:
                    next_time = actual_job_time
                    next_jobs = [job]

    if next_time is None:
        raise ValueError('No jobs found in the schedule.')
    return next_time, next_jobs


def schedule_job(next_time: datetime, sbatch_dir: str, job_name: str) -> None:
    sbatch_path = os.path.join(sbatch_dir, f'{job_name}.sbatch')
    begin_time = next_time.strftime('%Y-%m-%dT%H:%M:%S')
    tag = f'{job_name}@{begin_time}'
    logger.info('Scheduling %s at %s', sbatch_path, next_time)
    subprocess.run(
        ['sbatch', '--begin', begin_time, '--comment', tag, sbatch_path],
        check=True
    )


@click.command()
@click.option(
    '--debug/--no-debug', default=False,
    help='Set logging level to DEBUG.'
)
@click.option(
    '--schedule', '-s', required=True,
    help='Path to schedule file'
)
@click.option(
    '--sbatch-dir', '-d', required=True,
    help='Path to directory containing sbatch files'
)
def run(debug: bool, schedule: str, sbatch_dir: str) -> None:
    logging_setup(debug)

    with open(schedule, 'rb') as f:
        schedule_data = tomllib.load(f)

    next_time, next_jobs = find_next(schedule_data)

    next_schedule = next_time + timedelta(minutes=1)
    schedule_job(next_schedule, sbatch_dir, 'feder-scheduler')

    for job in next_jobs:
        schedule_job(next_time, sbatch_dir, job)


if __name__ == '__main__':
    run()
