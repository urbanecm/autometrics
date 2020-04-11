#!/usr/bin/env python
#-*- coding: utf-8 -*-

import re
from datetime import datetime
from dateutil import tz
import pywikibot
import toolforge
import sys

if len(sys.argv) <= 1:
	print('Usage: python3 update.py target [date]')
	sys.exit(1)

projects = {
	'cswiki': 'w:cs',
	'cswiktionary': 'wikt:cs',
	'cswikisource': 's:cs',
	'cswikibooks': 'b:cs',
	'cswikiquote': 'q:cs',
	'cswikinews': 'n:cs',
	'cswikiversity': 'v:cs'
}
targets = {
	'praha-2019': ('wikipedia', 'cs', 4, 'Klub/Praha/2019')
}
timezone = 'Europe/Prague'

target = targets[sys.argv[1]]
site = pywikibot.Site(target[1], target[0])
page = pywikibot.Page(site, target[3], ns=target[2])
text = page.text

secname = sys.argv[2] if len(sys.argv) > 2 else ''
datere = re.compile(r'(20[0-9][0-9])-([0-1]?[0-9])-([0-3]?[0-9])')
if secname is None or secname == '':
	for match in datere.finditer(text):
		pass
	date = '%04d%02d%02d' % tuple(map(int, match.groups('0')))
	pos = match.end()
else:
	match = datere.match(secname)
	date = '%04d%02d%02d' % tuple(map(int, match.groups('0')))
	pos = text.find(secname)

timere = re.compile(r'([0-2]?[0-9]):([0-5]?[0-9])')
time = ()
for match in timere.finditer(text, pos):
	time += ('%02d%02d' % tuple(map(int, match.groups('0'))),)
	if len(time) == 2:
		break

utcstamp = tuple([datetime.strptime(date+x+'00', '%Y%m%d%H%M%S').replace(tzinfo=tz.gettz(timezone)).astimezone(tz.gettz('UTC')).strftime('%Y%m%d%H%M%S') for x in time])

nextsecre = re.compile(r'^ *=', re.M)
match = nextsecre.search(text, pos)
nextsec = len(text) if not match else match.start()

usersre = re.compile(r'{{ *[Uu] *\| *([^}\|]*[^}\| ]) *[}\|]')
users = set([(x[0].upper()+x[1:]).replace('_', ' ') for x in usersre.findall(text, pos, nextsec)])

insre = re.compile(r'[Mm]etriky[^\n]*')
inspos = insre.search(text, pos).end()

print('Time', utcstamp)
print('Users', users)

metrics = {}
for project in projects:
	if project == 'commonswiki':
		continue
	print('Project', project)
	metrics[project] = {}
	conn = toolforge.connect(project, cluster='analytics')
	users_fmt_str = ', '.join(['%s'] * len(users))

	print('%s: Active editors' % project)
	with conn.cursor() as cur:
		params = tuple(users) + (utcstamp[0], utcstamp[0])
		q = '''
		SELECT actor_name
		FROM actor_revision JOIN revision ON rev_actor=actor_id
		WHERE actor_name IN (%s) AND
		rev_timestamp BETWEEN DATE_FORMAT(%%s - INTERVAL 30 DAY, '%%%%Y%%%%m%%%%d%%%%H%%%%i00') AND %%s
		GROUP BY actor_id
		HAVING COUNT(*) > 5
		''' % users_fmt_str
		cur.execute(q, params)
		data = cur.fetchall()
	metrics[project]['activeeditors'] = len(data)
	
	print('%s: New editors' % project)
	with conn.cursor() as cur:
		q = '''
		SELECT actor_name
		FROM actor_logging JOIN logging ON log_actor=actor_id
		WHERE log_action="newusers" AND log_type="create" AND
		actor_name IN (%s) AND
		log_timestamp BETWEEN DATE_FORMAT(%%s - INTERVAL 14 DAY, '%%%%Y%%%%m%%%%d%%%%H%%%%i00') AND %%s
		''' % users_fmt_str
		cur.execute(q, tuple(users) + (utcstamp[1], utcstamp[1]))
		data = cur.fetchall()
	metrics[project]['newusers'] = len(data)

	print('%s: Edits' % project)
	with conn.cursor() as cur:
		q = '''
		SELECT COUNT(DISTINCT rev_actor), COUNT(DISTINCT rev_page), COUNT(*)
		FROM revision
		JOIN actor_revision ON actor_id=rev_actor
		JOIN page ON rev_page=page_id
		WHERE actor_name IN (%s) AND
		page_namespace=0 AND
		rev_timestamp BETWEEN %%s AND %%s
		''' % users_fmt_str
		cur.execute(q, tuple(users) + (utcstamp[0], utcstamp[1]))
		data = cur.fetchall()
	metrics[project]['editing_editors'] = data[0][0]
	metrics[project]['edited_pages'] = data[0][1]
	metrics[project]['edits'] = data[0][2]

	print('%s: Bytes' % project)
	with conn.cursor() as cur:
		q = '''
		SELECT
			rev_actor,
			SUM(ABS(byte_change)) AS absolute_sum,
			SUM(CASE
				WHEN (byte_change>0)
				THEN byte_change
				ELSE 0 END
			) AS positive_sum,
			SUM(CASE
				WHEN (byte_change<0)
				THEN ABS(byte_change)
				ELSE 0 END
			) AS negative_sum,
			SUM(is_new) AS created_pages
		FROM
		(
			SELECT
				revision.rev_actor,
				cast(revision.rev_len as signed)-cast(coalesce(old_revision.rev_len, 0) as signed) AS byte_change,
				revision.rev_parent_id=0 AS is_new
			FROM revision
			JOIN actor_revision ON actor_id=revision.rev_actor
			JOIN page on page_id=revision.rev_page
			LEFT JOIN revision AS old_revision ON old_revision.rev_id=revision.rev_parent_id
			WHERE actor_name IN (%s) AND
			page_namespace = 0 AND
			revision.rev_timestamp BETWEEN %%s AND %%s
		)	AS anon_1 GROUP BY rev_actor;
		''' % users_fmt_str
		cur.execute(q, tuple(users) + tuple(utcstamp))
		data = cur.fetchall()
	metrics[project]['editingeditors'] = len(data)
	metrics[project]['absolute_sum'] = sum([x[1] for x in data])
	metrics[project]['positive_sum'] = sum([x[2] for x in data])
	metrics[project]['negative_sum'] = sum([x[3] for x in data])
	metrics[project]['new_pages'] = sum([x[4] for x in data])
	metrics[project]['creating_users'] = len([x[4] for x in data])

# process commonswiki
if 'commonswiki' in projects:
	conn = toolforge.connect('commonswiki')
	metrics['commonswiki'] = {}
	with conn.cursor() as cur:
		q = '''
		SELECT actor_name, COUNT(*)
		FROM revision
		JOIN actor_revision ON actor_id=rev_actor
		JOIN page on page_id=rev_page
		WHERE actor_name IN (%s) AND
		page_namespace = 6 AND
		rev_parent_id = 0 AND
		revision.rev_timestamp BETWEEN %%s AND %%s
		GROUP BY rev_actor;
		''' % users_fmt_str
		cur.execute(q, tuple(users) + tuple(utcstamp))
		data = cur.fetchall()
	metrics['commonswiki']['files_uploaded'] = sum([x[1] for x in data])
	metrics['commonswiki']['files_users'] = len(data)

print(metrics)

# this function takes a word (e.g. noun or adjective) in its base form, along with a number, and returns the form of the word appropriate for the number:
def morph(num, words, prepend = True, prependbold = True):
	morphwords = []
	for word in words.split():
		morphwords.append({
			u'zúčastněný': lambda x: u'zúčastněný' if x == 1 else (u'zúčastnění' if 2 <= x <= 4 else u'zúčastněných'),
			u'editor': lambda x: u'editor' if x == 1 else (u'editoři' if 2 <= x <= 4 else u'editorů'),
			u'aktivní': lambda x: u'aktivní' if 1 <= x <= 4 else u'aktivních',
			u'nově': lambda x: u'nově',
			u'registrovaný': lambda x: u'registrovaný' if x == 1 else (u'registrovaní' if 2 <= x <= 4 else u'registrovaných'),
			u'byte': lambda x: u'byte' if x == 1 else (u'byty' if 2 <= x <= 4 else u'bytů'),
			u'přidaný': lambda x: u'přidaný' if x == 1 else (u'přidané' if 2 <= x <= 4 else u'přidaných'),
			u'odebraný': lambda x: u'odebraný' if x == 1 else (u'odebrané' if 2 <= x <= 4 else u'odebraných'),
			u'editace-6': lambda x: u'editaci' if x == 1 else u'editacích',
			u'provedená-6': lambda x: u'provedené' if x == 1 else u'provedených',
			u'uživatel-7': lambda x: u'uživatelem' if x == 1 else u'uživateli',
			u'editace': lambda x: u'editace' if 1 <= x <= 4 else u'editací',
			u'provedená': lambda x: u'provedená' if x == 1 else (u'provedené' if 2 <= x <= 4 else u'provedených'),
			u'editovaný': lambda x: u'editovaný' if x == 1 else (u'editované' if 2 <= x <= 4 else u'editovaných'),
			u'článek': lambda x: u'článek' if x == 1 else (u'články' if 2 <= x <= 4 else u'článků'),
			u'založený': lambda x: u'založený' if x == 1 else (u'založené' if 2 <= x <= 4 else u'založených'),
			u'stávající': lambda x: u'stávající' if 1 <= x <= 4 else u'stávajících',
			u'soubor': lambda x: u'soubor' if x == 1 else (u'soubory' if 2 <= x <= 4 else u'souborů'),
			u'nahraný': lambda x: u'nahraný' if x == 1 else (u'nahrané' if 2 <= x <= 4 else u'nahraných'),
		}[word](num))
	return ('' if not prepend else ''.join([u"'''" if prependbold else '', u"{{subst:formatnum:%d}}" % num, u"'''" if prependbold else '', u' '])) + ' '.join(morphwords)

metrics_text = "\n"

activeusers = sum([metrics[x]['activeeditors'] for x in metrics])
newusers = sum([metrics[x]['newusers'] for x in metrics])
metrics_text += "* %s (v tom %s a %s)\n" % (morph(len(users), 'zúčastněný editor'), morph(activeusers, 'aktivní'), morph(newusers, 'nově registrovaný'))

totalabssum = sum([metrics[x]['absolute_sum'] for x in metrics])
totalpossum = sum([metrics[x]['positive_sum'] for x in metrics])
totalnegsum = sum([metrics[x]['negative_sum'] for x in metrics])
totaledits = sum([metrics[x]['edits'] for x in metrics])
totaleditingeditors = sum([metrics[x]['editing_editors'] for x in metrics])
metrics_text += "* %s součet změn (=%s + %s) v %s %s" % (morph(totalabssum, 'byte'), morph(totalpossum, 'přidaný'), morph(totalnegsum, 'odebraný'), morph(totaledits, 'editace-6 provedená-6'), morph(totaleditingeditors, 'uživatel-7'))

if totalabssum > 0:
	metrics_text += ', a to:'
metrics_text += '\n'

for project in projects:
	if metrics[project]['edits'] == 0 or project == 'commonswiki':
		continue
	interwiki = projects[project]
	metrics_text += "** na [[:%s|%s]]: %s %s – v tom %s (=%s + %s)\n" % (interwiki, project, morph(metrics[project]['edits'], 'editace-6 provedená-6'), morph(metrics[project]['editing_editors'], 'uživatel-7'), morph(metrics[project]['edited_pages'], 'editovaný článek'), morph(metrics[project]['new_pages'], 'nově založený'), morph(metrics[project]['edited_pages']-metrics[project]['new_pages'], 'stávající'))

# special handling for commonswiki
if 'commonswiki' in projects and metrics['commonswiki']['files_uploaded'] > 0:
	metrics_text += "** na [[:c:|commonswiki]]: %s %s" % (morph(metrics['commonswiki']['files_uploaded'], 'soubor nahraný'), morph(metrics['commonswiki']['files_users'], 'uživatel-7'))

newtext = text[:inspos] + metrics_text.rstrip("\n") + text[inspos:]

print('Generated report:')
pywikibot.showDiff(text, newtext)

print('Saving...')
page.put(newtext, 'automaticky generované metriky za den %s' % date, minorEdit=False)
