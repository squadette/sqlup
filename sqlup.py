# -*- coding: utf-8 -*-

import sys, os, re, pymssql, ConfigParser
from optparse import OptionParser

SCHEMA_INFO = 'schema_info'

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

"""
PROC_DIR = 'procs'

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

def save_proc(proc):
	fname = '%s%s%s.sql' % (PROC_DIR, os.sep, proc['name'])
	print 'writing %s' % fname
	f = open(fname, 'w')
	f.write(proc['definition'])
	f.close()
"""

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
	schema_info = cursor.fetchone()
	print 'Schema info: version %i, last update %s' % (schema_info[0], schema_info[1]) 
	scripts = get_scripts(db_dir)
	#~ print dir(cur)
	#~ print scripts['migration']
	for script in scripts['migration']:
		pass
		cursor.execute(script['sql'])
	
	query = "update %s set schema_version = schema_version +1, last_update = getdate()" % SCHEMA_INFO
	cursor.execute(query)

def get_scripts(dir):
	def cmp(f1, f2):
		try:
			n1 = int(re.sub(r'\D', '', f1))
			n2 = int(re.sub(r'\D', '', f2))
		except ValueError:
			print "Error: incorrect file names: %s, %s" % (f1, f2)
			sys.exit(1)
		return n1 - n2
		
	func_dir = 'functions'
	proc_dir = 'procedures'
	migr_dir = 'migration'
	ret = {
		'migration': [ ],
		'procedures': [ ],
		'functions': [ ],
	}
	for script in listdir(dir + os.sep + migr_dir):
		file = open(dir + os.sep + migr_dir + os.sep + script)
		ret['migration'].append({
			'script': script,
			'sql': ''.join(file.readlines()),
		})
		ret['migration'].sort(cmp, lambda elem: elem['script'])
	return ret

def main():
	parser = OptionParser(usage="usage: %prog [-c CONFIG] action schema_directory")
	parser.add_option('-c', '--conf', dest='config', default='sqlup.conf', help='config file to use. Default is "%default"')
	(options, args) = parser.parse_args()
	if len(sys.argv[1:]) == 0 or len(args) != 2:
		parser.print_help()
	schema_dir = args[1]
	options = get_options(options.config)
	servers = options['servers']
	#~ TODO: кто обнуляет SCHEMA_INFO?
	#~ SCHEMA_INFO = options['schema_table']
	migrate(servers, schema_dir)

if __name__ == '__main__':
	main()

#~ con = pymssql.connect(host='192.168.21.12', user='supervisor', password='tg64th', database='AdventureWorks')




#~ query="create table pymssql (col1 int);"
#~ cur.execute(query)
#~ print "create table: %d" % cur.rowcount
#~ con.commit()
