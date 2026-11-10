import subprocess, json, os
W = 'I:/OpenClaw-maintenance/weekly/hindsight-stack'
BASE = '85ab32ebe'
MAP_FILES = {'stack-tests.json', 'stack-tests.gen.py'}
def git(*a):
    return subprocess.run(['git', '-C', W, '-c', 'safe.directory=*'] + list(a), capture_output=True, text=True)
d = json.load(open(os.path.join(W, 'stack-tests.json')))
rows = {r['key']: r for r in d['commits']}
lines = git('log', '--format=%H%x00%s', BASE + '..HEAD').stdout.strip().splitlines()
matched = missing = mismatch = skipped = 0
for line in lines:
    sha, _, subject = line.partition('\x00')
    files = [f for f in git('show', '--name-only', '--format=', sha).stdout.splitlines() if f]
    if files and set(files) <= MAP_FILES:
        skipped += 1
        continue
    diff = subprocess.run(['git', '-C', W, 'show', sha, '--format='], capture_output=True).stdout
    pid = subprocess.run(['git', '-C', W, 'patch-id', '--stable'], input=diff, capture_output=True).stdout.decode().split()
    row = rows.get(subject)
    if not row:
        missing += 1
    elif not pid or row.get('patch_id') != pid[0]:
        mismatch += 1
    else:
        matched += 1
print('commits_checked', matched + missing + mismatch, 'matched', matched, 'missing_row', missing, 'patch_id_mismatch', mismatch, 'skipped_map_commits', skipped)
print('rows', len(rows))