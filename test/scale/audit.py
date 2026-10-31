import json, sys, collections
per_sec = collections.defaultdict(collections.Counter)
verbs = collections.defaultdict(collections.Counter)
for line in open(sys.argv[1]):
    try: e = json.loads(line)
    except Exception: continue
    if e.get('stage') != 'ResponseComplete': continue
    user = e['user']['username'].split(':')[2] if e['user']['username'].count(':') >= 3 else e['user']['username']
    if e['verb'] == 'watch': continue
    per_sec[user][e['requestReceivedTimestamp'][:19]] += 1
    verbs[user][(e.get('objectRef', {}).get('resource', '?') + ' ' + e['verb'])] += 1
print(json.dumps({u: {'peakRequestsPerSecond': max(c.values()), 'requests': sum(c.values()), 'byResourceVerb': dict(verbs[u])} for u, c in per_sec.items()}, indent=1))
