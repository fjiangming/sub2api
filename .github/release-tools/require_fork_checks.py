#!/usr/bin/env python3
"""Keep fork images and releases behind checks for their exact source commit."""
import json
import os
import re
import sys
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen


WORKFLOWS = ('backend-ci.yml', 'security-scan.yml')


def workflow_status(runs, workflow, sha):
    matching = [run for run in runs
                if run.get('head_sha') == sha
                and run.get('head_branch') == 'dev'
                and run.get('event') == 'push'
                and run.get('path') == f'.github/workflows/{workflow}']
    if not matching:
        return 'missing'
    latest = max(matching, key=lambda run: run['id'])
    if latest.get('status') != 'completed':
        return latest.get('status') or 'pending'
    if latest.get('conclusion') != 'success':
        raise RuntimeError(f"{workflow} did not pass for {sha}: {latest.get('conclusion')}")
    return 'success'


def wait_for_checks(fetch_runs, sha, timeout=2400, interval=20,
                    clock=time.monotonic, sleep=time.sleep):
    deadline = clock() + timeout
    previous = None
    while clock() < deadline:
        statuses = {workflow: workflow_status(fetch_runs(workflow), workflow, sha)
                    for workflow in WORKFLOWS}
        if all(status == 'success' for status in statuses.values()):
            print(f'Required checks passed for {sha}', flush=True)
            return
        if statuses != previous:
            print(f'Waiting for checks on {sha}: {statuses}', flush=True)
            previous = statuses
        sleep(interval)
    raise RuntimeError(f'Timed out waiting for required checks on {sha}')


def main():
    repository = os.environ.get('GITHUB_REPOSITORY', '')
    sha = os.environ.get('GITHUB_SHA', '')
    token = os.environ.get('GH_TOKEN', '')
    if repository != 'fjiangming/sub2api' or not re.fullmatch(r'[0-9a-f]{40}', sha) or not token:
        raise RuntimeError('Expected the fork repository, a full commit SHA and GH_TOKEN')

    def fetch_runs(workflow):
        query = urlencode({'head_sha': sha, 'branch': 'dev', 'event': 'push', 'per_page': 100})
        request = Request(
            f'https://api.github.com/repos/{repository}/actions/workflows/{workflow}/runs?{query}',
            headers={'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json',
                     'User-Agent': 'sub2api-fork-release'})
        with urlopen(request, timeout=30) as response:
            return json.load(response)['workflow_runs']

    wait_for_checks(fetch_runs, sha)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'Release blocked: {error}', file=sys.stderr)
        sys.exit(1)
