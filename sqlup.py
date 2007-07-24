import sys, os, pymssql, ConfigParser
from optparse import OptionParser

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
	for server in os.listdir(schema_dir):
		for db in os.listdir(schema_dir + os.sep + server):
			db_dir = schema_dir + os.sep + server + os.sep + db
			conf = servers[server]
			conf['database'] = db
			migrate_db(db_dir, conf)

def migrate_db(db_dir, conf):
	con = pymssql.connect(**conf)
	cur = con.cursor()
	query = "select * from SchemaInfo"
	cur.execute(query)
	print 'Schema info:', cur.fetchone()
	scripts = get_scripts(db_dir)
	#~ print dir(cur)
	for script in scripts['migration']:
		cur.execute(script)
	con.commit()
	con.close()

def get_scripts(dir):
	func_dir = 'functions'
	proc_dir = 'procedures'
	migr_dir = 'migration'
	ret = {
		'migration': [ ],
	}
	for script in os.listdir(dir + os.sep + migr_dir):
		file = open(dir + os.sep + migr_dir + os.sep + script)
		ret['migration'].append(''.join(file.readlines()))
	return ret

def main():
	parser = OptionParser(usage="usage: %prog [-c CONFIG] action schema_directory")
	parser.add_option('-c', '--conf', dest='config', default='sqlup.conf', help='config file to use. Default is "%default"')
	(options, args) = parser.parse_args()
	if len(sys.argv[1:]) == 0 or len(args) != 2:
		parser.print_help()
	schema_dir = args[1]
	servers = get_options(options.config)['servers']
	migrate(servers, schema_dir)

if __name__ == '__main__':
	main()

#~ con = pymssql.connect(host='192.168.21.12', user='supervisor', password='tg64th', database='AdventureWorks')




#~ query="create table pymssql (col1 int);"
#~ cur.execute(query)
#~ print "create table: %d" % cur.rowcount
#~ con.commit()
