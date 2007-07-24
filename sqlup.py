# -*- coding: utf-8 -*-

import sys, os, re, pymssql, ConfigParser
from optparse import OptionParser

SCHEMA_INFO = 'schema_info'
PROC_DIR = 'procedures'
FUNC_DIR = 'functions'
MIGR_DIR = 'migration'

def listdir(dir):
	ret = [ ]
	for str in os.listdir(dir):
		if not str.startswith('.'):
			ret.append(str)
	return ret

def get_options(filename):
	try:
		cnfFile = open(filename)
	except IOError:
		print "Error: cannot open config file %s" % filename
		sys.exit(1)

	servers = { }
	cnf = ConfigParser.ConfigParser()
	cnf.readfp(cnfFile)
	try:
		for section in cnf.sections():
			servers[section] = {
				'host': cnf.get(section, 'host'),
				'user': cnf.get(section, 'user'),
				'password': cnf.get(section, 'password'),
			}
	except ConfigParser.NoOptionError, e:
		print "Config file error: %s" % e
		sys.exit(1)

	ret = {
		'servers': servers,
	}
	ret.update(cnf.defaults())
	return ret

def dump_procs(cur):
	ret = []
	query = 'SELECT specific_name, CAST(routine_definition AS text), last_altered FROM INFORMATION_SCHEMA.ROUTINES'
	cur.execute(query)
	for proc in cur.fetchall():
		ret.append({
			'name': proc[0],
			'definition': proc[1],
			'last_altered': proc[2],
		})
	return ret

def save_proc(dir, proc):
	fname = dir + os.sep + proc['name'] + '.sql'
	print 'writing %s' % fname
	f = open(fname, 'w')
	f.write(proc['definition'])
	f.close()

def migrate(servers, schema_dir):
	print 'migrating...'
	for server in listdir(schema_dir):
		for db in listdir(schema_dir + os.sep + server):
			db_dir = schema_dir + os.sep + server + os.sep + db
			conf = servers[server]
			conf['database'] = db
			con = pymssql.connect(**conf)
			cur = con.cursor()
			find_collision(cur)
			migrate_db(db_dir, cur)
			con.commit()
			con.close()

def find_collision(cursor):
	query = 'select specific_name, last_altered from INFORMATION_SCHEMA.ROUTINES t, schema_info s where t.last_altered > s.last_update'
	cursor.execute(query)
	if cursor.rowcount > 0:
		for data in cursor.fetchall():
			print "Collistion detected: stored procedure %s was altered after last schema update" % data[0]
		sys.exit(1)

def migrate_db(db_dir, cursor):
	query = "select * from %s" % SCHEMA_INFO
	cursor.execute(query)
	(db_version, last_update) = cursor.fetchone()
	scripts = get_scripts(db_dir)
	to_version = extract_version(scripts[MIGR_DIR][-1]['script'])
	if (to_version <= db_version):
		print 'Noting to update (db version %i, current version %i' % (db_version, to_version)
		return
	print 'Schema info: version %i, last update %s, updating to version %i' % (db_version, last_update, to_version)
	for script in scripts[MIGR_DIR]:
		script_version = extract_version(script['script'])
		if script_version > db_version:
			print 'runnig script %s' % script['script']
			cursor.execute(script['sql'])
		
	for script in scripts[PROC_DIR]:
		proc_name = os.path.splitext(script['script'])[0]
		#~ TODO: почему подстановка ? не работает в pymssql?
		#~ query = 'SELECT specific_name FROM INFORMATION_SCHEMA.ROUTINES where specific_name = ?'
		#~ cursor.execute(query, (proc_name)
		query = "SELECT specific_name FROM INFORMATION_SCHEMA.ROUTINES where specific_name = '%s'" % proc_name
		cursor.execute(query)
		if cursor.rowcount > 0:
			print "dropping procedure %s" % proc_name
			query = "drop procedure %s" % proc_name
			cursor.execute(query)
		print "creating procedure %s" % proc_name
		cursor.execute(script['sql'])
	
	print 'Updating %s table' % SCHEMA_INFO
	if script_version > db_version:
		query = 'update %s set schema_version = %i, last_update = getdate()' % (SCHEMA_INFO, to_version)
	else:
		query = 'update %s set last_update = getdate()' % SCHEMA_INFO
		cursor.execute(query)

def extract_version(file):
	try:
		version = int(re.sub(r'\D', '', file))
	except ValueError:
		print "Error: incorrect file name: %s" % file
		sys.exit(1)
	return version

def get_scripts(dir):
	def cmp(f1, f2):
		n1 = extract_version(f1)
		n2 = extract_version(f2)
		return n1 - n2
		
	ret = {
		MIGR_DIR: [ ],
		PROC_DIR: [ ],
		FUNC_DIR: [ ],
	}
	for type in ret:
		for script in listdir(dir + os.sep + type):
			file = open(dir + os.sep + type + os.sep + script)
			ret[type].append({
				'script': script,
				'sql': ''.join(file.readlines()),
			})
			ret[type].sort(cmp, lambda elem: elem['script'])
	return ret

def action_migrate(options, args, config):
	schema_dir = args[0]
	servers = config['servers']
	#~ TODO: кто обнуляет SCHEMA_INFO?
	#~ SCHEMA_INFO = options['schema_table']
	migrate(servers, schema_dir)

def action_dump(options, args, config):
	con = pymssql.connect(database=options.database, **config['servers'][options.database])
	cur = con.cursor()
	procs = dump_procs(cur)
	con.close()
	dir = args[0] + os.sep + PROC_DIR
	if not os.path.exists(dir):
		os.makedirs(dir)
	for proc in procs:
		save_proc(dir, proc)


def main():
	parser = OptionParser(usage="usage: %prog [-c CONFIG] [-d DATABASE] {migrate|dump} schema_directory")
	parser.add_option('-c', '--conf', dest='config', default='sqlup.conf', help='config file to use. Default is "%default"')
	parser.add_option('-d', '--database', dest='database', help='database name, used with action "dump"')
	(options, args) = parser.parse_args()
	config = get_options(options.config)
	if len(sys.argv[1:]) == 0 or len(args) != 2:
		parser.print_help()
		sys.exit()

	actions = {
		'migrate': action_migrate,
		'dump': action_dump,
	}
	actions[args[0]](options, args[1:], config)

if __name__ == '__main__':
	main()
