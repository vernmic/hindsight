#!/usr/bin/env python3
"""Regenerate stack-tests.json for the Hindsight stack.

Base is the Hindsight baseline 85ab32ebe, so the map covers every commit the weekly runner walks:
the server line and the plugin replay. Rows are keyed by subject plus patch-id (stable across
rebase). Commits that only touch the map itself are excluded: the map does not describe itself.
A commit with no mapped test must carry a no_test_reason."""
import subprocess, json, os, re, sys

W = os.path.dirname(os.path.abspath(__file__))
BASE = '85ab32ebe'
MAP_FILES = {'stack-tests.json', 'stack-tests.gen.py', 'stack-tests.verify.py'}

def git(*a):
    return subprocess.run(['git', '-C', W, '-c', 'safe.directory=*'] + list(a), capture_output=True, text=True)

SERVER_TESTS = {
    'consolidation': 'hindsight-api-slim/tests/test_consolidation_preserve_tag_prefixes.py',
    'append_fallback': 'hindsight-api-slim/tests/test_retain_append_fallback_keeps_tags.py',
    'append_precision': 'hindsight-api-slim/tests/test_append_tag_carry_precision.py',
    'append': 'hindsight-api-slim/tests/test_retain_append_preserves_unit_tags.py',
    'prefix': 'hindsight-api-slim/tests/test_retain_preserve_tag_prefixes.py',
}

def is_test_file(f):
    b = os.path.basename(f)
    return ('/tests/' in f or '/test/' in f or b.startswith('test_') or b.endswith('_test.py')
            or b.endswith('.test.ts') or b.endswith('.spec.ts'))

def server_tests(low):
    picked = []
    if 'consolidation' in low: picked.append(SERVER_TESTS['consolidation'])
    if 'append' in low and 'fallback' in low: picked.append(SERVER_TESTS['append_fallback'])
    if 'append' in low and 'precision' in low: picked.append(SERVER_TESTS['append_precision'])
    if 'append' in low and 'fallback' not in low and 'precision' not in low: picked.append(SERVER_TESTS['append'])
    if 'prefix' in low: picked.append(SERVER_TESTS['prefix'])
    if not picked and ('retain' in low or 'tag' in low or 'preserve' in low):
        picked = list(SERVER_TESTS.values())
    return picked

def reason(files):
    if not files: return 'merge or empty commit'
    exts = {os.path.splitext(f)[1] for f in files}
    if exts <= {'.md', '.txt', '.rst'}: return 'docs-only'
    if any(f.endswith('package-lock.json') for f in files): return 'lockfile only'
    if all(f.endswith('.json') for f in files): return 'manifest/schema description'
    return 'preparatory or type-only change with no direct test'

tracked = set(git('ls-files').stdout.splitlines())
lines = git('log', '--reverse', '--format=%H%x00%s', BASE + '..HEAD').stdout.strip().splitlines()
rows = []
skipped = 0
for line in lines:
    sha, _, subject = line.partition('\x00')
    files = [f for f in git('show', '--name-only', '--format=', sha).stdout.splitlines() if f]
    if files and set(files) <= MAP_FILES:
        skipped += 1
        continue
    diff = subprocess.run(['git', '-C', W, 'show', sha, '--format='], capture_output=True).stdout
    pid = subprocess.run(['git', '-C', W, 'patch-id', '--stable'], input=diff, capture_output=True).stdout.decode().split()
    tests = set(f for f in files if f in tracked and is_test_file(f))  # own tests first, every area
    if not tests:
        low = (' '.join(files) + ' ' + subject).lower()
        if any(f.startswith('hindsight-integrations/openclaw') for f in files):
            for f in files:
                stem = re.sub(r'\.(ts|js|json)$', '', f)
                cand = stem + '.test.ts'
                if cand in tracked: tests.add(cand)
        elif any(f.startswith('hindsight-api-slim') for f in files):
            tests.update(server_tests(low))
        elif any(f.startswith('hindsight-integrations/hermes') for f in files):
            pool = [f for f in tracked if f.startswith('hindsight-integrations/hermes/tests')]
            tokens = {os.path.splitext(os.path.basename(f))[0].replace('_', '-') for f in files}
            matched = [t for t in pool if any(tok and tok in t.replace('_', '-') for tok in tokens)]
            tests.update(matched or pool)
    row = {'key': subject, 'patch_id': pid[0] if pid else '', 'files': files, 'tests': sorted(tests)}
    if not tests:
        row['no_test_reason'] = reason(files)
    rows.append(row)

json.dump({'base': BASE, 'keyed_by': 'subject+patch-id', 'commits': rows},
          open(os.path.join(W, 'stack-tests.json'), 'w'), indent=1)
print('rows', len(rows), 'skipped_map_commits', skipped, 'with_tests', sum(1 for r in rows if r['tests']),
      'no_reason_gaps', sum(1 for r in rows if not r['tests'] and not r.get('no_test_reason')))
print('empty_patch_ids', sum(1 for r in rows if not r['patch_id']))