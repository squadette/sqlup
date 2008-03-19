# -*- coding: utf-8 -*-
__version__ = "$Id$"

import sys, os, re, pymssql, ConfigParser
from pymssql import DatabaseError
from datetime import datetime
from optparse import OptionParser


PROC_DIR = 'procedures'
FUNC_DIR = 'functions'
MIGR_DIR = 'migration'
SQLUP_CUT='-- SQLUP-CUT'

def listdir(dir, ext=None):
	"""
	Замена стандартному os.listdir, пропускает файлы с именами начинающимися с '.'
	если задан параметр ext, то возвращает файлы только с этим расширением
	"""
	ret = [ ]
	if os.path.isdir(dir):
		for str in os.listdir(dir):
			if str.startswith('.'):
				continue
			if ext and not str.endswith(ext):
				continue
			ret.append(str)
	return ret

def get_config(filename):
	"""
	Читает конфиг-файл
	"""
	try:
		cnfFile = open(filename)
	except IOError:
		print "Error: cannot open config file %s" % filename
		return False

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

def dump_routines(cur, type):
	"""
	Выбирает из information_schema.routines объекты указанного типа, возвращает массивом
	(выбираются только объекты с routine_body = 'SQL')
	"""
	
	ret = []
	query = """
		SELECT specific_name, last_altered
		FROM INFORMATION_SCHEMA.ROUTINES
		WHERE routine_body = 'SQL' 
			AND specific_schema = 'dbo' 
			AND routine_type = %s 
			AND specific_name NOT LIKE 'dt_%%' 
			AND specific_name NOT LIKE 'sp_%%'"""
	#print query
	cur.execute(query, (type,))
	for proc in cur.fetchall():
		regex = re.compile(r'create (proc|procedure|function) ', re.I)
		definition = regex.sub(r'ALTER \1 ', get_routine_definition(cur, proc[0]))
		ret.append({
			'name': proc[0],
			'definition': definition,
			'last_altered': proc[1],
		})
	return ret

def get_routine_definition(cur, name):
	definition = ""
	cur.execute('set textsize 1000000')
	query = """SELECT CAST(c.text AS TEXT) FROM syscomments c WHERE c.id = (
			SELECT object_id FROM sys.all_objects
			WHERE name = %s
		) ORDER BY c.colid"""
	cur.execute(query, (name,))
	definition = ''.join([row[0] for row in cur.fetchall()])
	definition = '\n'.join(definition.splitlines())
	
	print name, len(definition)
	return definition

def save_routine(dir, proc):
	"""
	Сохраняет код процедуры или функции в файле в указанной директории
	"""
	fname = dir + os.sep + proc['name'] + '.sql'
	print 'writing %s' % fname
	f = open(fname, 'w')
	f.write(proc['definition'])
	f.close()

def migrate(servers, schema_dir, rollback=False, ignore=False, to_version=None, skip=[]):
	"""
	Выполняет миграцию схемы из данной директории на сервера из списка
	"""
	
	print 'migrating...'
	for server in listdir(schema_dir):
		for db in listdir(schema_dir + os.sep + server):
			db_dir = schema_dir + os.sep + server + os.sep + db
			if rollback:
				scripts = get_scripts(db_dir, reverse=True)
			else:
				scripts = get_scripts(db_dir)
				if len(scripts['migration']):
					to_version = extract_version(scripts[MIGR_DIR][-1]['script'])
			
			conf = servers[server + '.' + db]
			conf['database'] = db
			con = pymssql.connect(**conf)
			cur = con.cursor()
			
			(db_version, last_update) = schema_info(cur)
			print 'Schema info: version %i, last update %s' % (db_version, last_update.strftime('%Y-%m-%d %H:%M'))
			update_needed = (rollback and to_version < db_version) or (not rollback and to_version > db_version)
			
			tcoll = find_table_collision(cur)
			if tcoll:
				for coll in tcoll:
					print "Collistion detected: table %s was altered after last schema update" % coll[0]
				if not ignore:
					print "Use --ignore options to ignore collisions"
			
			if ignore:
				print "Ignoring collisions"
				tcoll = [ ]

			if tcoll and not skip:
				print 'To migrate schema while collision detected, use option "skip"'
			else:
				if update_needed:
					print 'Migrating database schema to version %i' % to_version
					if rollback:
						mig_scripts = refine_scripts(scripts[MIGR_DIR], to_version, db_version, skip)
						field = 'sqldown'
					else:
						mig_scripts = refine_scripts(scripts[MIGR_DIR], db_version + 1, to_version, skip)
						field = 'sqlup'
					run_scripts(mig_scripts, field, cur)
					print 'Updating schema_info table'
					query = 'update schema_info set schema_version = %i, last_update = getdate()' % to_version
					cur.execute(query)
				else:
					if to_version is None:
						print 'No migration scripts provided'
					else:
						print 'Database schema version %i, no need to update/rollback to version %i' % (db_version, to_version)

			rcoll = find_routine_collision(cur)
			if rcoll:
				for coll in rcoll:
					print "Collistion detected: stored procedure %s was altered after last schema update" % coll[0]
				if not ignore:
					print "Use --ignore options to ignore collisions"

			if ignore:
				print "Ignoring collisions"
				rcoll = [ ]
			
			if not rcoll:
				update_routines(scripts[PROC_DIR] + scripts[FUNC_DIR], cur)

			con.commit()
			con.close()

def refine_scripts(scripts, version1, version2, skip):
	"""
	Чистит список скриптов, полученный из директории, оставляя в нем
	только нужные миграции между указанными версиями, пропускает
	версии из списка skip
	"""
	ret = [ ]
	for script in scripts:
		v = script['version']
		if v >= version1 and v <= version2 and not v in skip:
			ret.append(script)
	return ret
		

def find_routine_collision(cursor):
	"""
	Возвращает список процедур и функций с временем изменения большим, 
	чем schema_info.last_update
	"""
	query = 'select specific_name, last_altered from INFORMATION_SCHEMA.ROUTINES t, schema_info s where t.last_altered > s.last_update'
	cursor.execute(query)
	return cursor.fetchall()

def find_table_collision(cursor):
	"""
	Возвращает список таблиц с временем изменения большим, 
	чем schema_info.last_update
	"""
	query = 'select name, modify_date from sys.tables t, schema_info s where t.modify_date > s.last_update'
	cursor.execute(query)
	return cursor.fetchall()

def schema_info(cursor):
	"""
	Возвращает список (версия, время_последнего апдейта) для текущей БД
	"""
	
	(db_version, last_update) = (-1, datetime.fromtimestamp(0))
	
	query = 'select * from schema_info'
	try:
		cursor.execute(query)
		if cursor.rowcount:
			(db_version, last_update) = cursor.fetchone()
		else:
			query = 'insert into schema_info(schema_version, last_update) values(-1, getdate())'
			cursor.execute(query)
			
	except DatabaseError, e:
		print "Error occured while reading schema_info table: %s" % e
		sys.exit(1)
		
	return (db_version, last_update)

def update_routines(scripts, cursor):
	"""
	Обновляет хранимые процедуры и функции в текущей БД
	"""
	print 'Updating routines:'
	for script in scripts:
		proc_name = os.path.splitext(script['script'])[0]
		query = 'SELECT specific_name FROM INFORMATION_SCHEMA.ROUTINES where specific_name = %s'
		cursor.execute(query, (proc_name,))
		if cursor.rowcount > 0:
			print '\taltering routine %s' % proc_name
			#print script['sql']
			cursor.execute(script['sql'])
	print 'Updating schema_info.last_update'
	query = 'update schema_info set last_update = getdate()'
	cursor.execute(query)

#~ def migrate_db(scripts, versions, cursor, field='sqlup'):
	#~ """
	#~ Основной код миграции: мигрирует схему из данной директории в одну базу. 
	#~ """
	
	#~ for script in scripts[MIGR_DIR]:
		#~ script_version = extract_version(script['script'])
		#~ if script_version in versions and field in script:
			#~ print '\trunnig %s from script %s' % (field, script['script'])
			#~ cursor.execute(script[field])

def run_scripts(scripts, field, cursor):
	"""
	Выполняет в базе код из поля field массива scripts
	"""
	for script in scripts:
		print '\trunnig %s from script %s' % (field, script['script'])
		cursor.execute('BEGIN TRAN')
		cursor.execute(script[field])
		cursor.execute('COMMIT')

def extract_version(file):
	"""
	Выдирает начальные цифры из имени файла и склеивает их в INT
	"""
	
	n = re.compile('^(\d+).*').match(file)
	if not n:
		print "Error: incorrect file name: %s" % file
		sys.exit(1)
	return int(n.groups()[0])

def get_scripts(dir, reverse=False):
	"""
	Читает список скриптов из dir, сортирует их по цифрам в имени,
	возвращает массив вида:
	{
		директория1:
			[
				{script: имя_файла, sql: код_скрипта},
				{...}
			],
		директория2: ...
	}
	"""
	
	ret = { }
	for type in listdir(dir):
		ret[type] = [ ]
		for script in listdir(dir + os.sep + type, '.sql'):
			file = open(dir + os.sep + type + os.sep + script)
			sql = ''.join(file.readlines())
			(sqlup, sqldown) = ('', '')
				
			data = {
				'script': script,
				'sql': sql,
				# для получения SQL-типа тупо обрезаем последнюю букву. It works for me
				'type': type[:-1],
			}
			
			if type == MIGR_DIR:
				data['version'] = extract_version(script)
				sql_parts = sql.split(SQLUP_CUT)
				data['sqlup'] = sql_parts[0]
				if len(sql_parts) > 1:
					data['sqldown'] = sql_parts[1]
					
			ret[type].append(data)
			
		if type == MIGR_DIR:
			ret[type].sort(cmp, lambda elem: elem['version'], reverse)

	return ret

class WantArgs:
	"""
	<СложнаяШтука>
	
	Это сделано, чтобы не проверять по отдельности в каждом action'е,
	сколько ему пришло параметров. Экземпляр класса - это callable-выражение,
	при вызове возвращающее функцию-декоратор, которая знает, сколько элементов
	в массиве args должно придти декорируемой фунции.
	
	</СложнаяШтука>
	"""
	
	def __init__(self, n):
		self.n = n
	
	def __call__(self, f):
		def check(options, args, config):
			if len(args) != self.n:
				print 'Error: this action requires %i argument(s)' % self.n
				return
			f(options, args, config)
		return check

def validate_schema_dir(config, dir):
	valid = True
	for server, db in [s.split('.') for s in config['servers']]:
		server_dir = os.path.join(dir, server)
		if not os.path.isdir(server_dir):
			valid = False
			print 'Error: directory for server "%s" (listed in config file) not found in schema directory %s' % (server, dir)
		db_dir = os.path.join(server_dir, db)
		if not os.path.isdir(db_dir):
			valid = False
			print 'Error: directory for db "%s" (listed in config file) not found in server directory %s' % (db, server_dir)
		for d in (PROC_DIR, FUNC_DIR, MIGR_DIR):
			subdir = os.path.join(db_dir, d)
			if not os.path.isdir(subdir):
				valid = False
				print 'Error: necessary directory "%s" not found in db directory %s' % (db_dir, subdir)
	return valid


def action_migrate(options, args, config):
	if len(args) < 1:
		print 'Error: migrate requires minimum 1 argument'
		return
	schema_dir = args[0]
	if not validate_schema_dir(config, schema_dir):
		print 'Error: invalid schema directory structure'
		return
	servers = config['servers']
	skip = [ ]
	try:
		if len(args) > 1 and args[1] == 'skip':
			skip = [int(a) for a in args[2:]]
	except ValueError, e:
		print 'Error: version number must be integer'
		return
		
	migrate(servers, schema_dir, ignore=options.ignore, skip=skip)

def action_rollback(options, args, config):
	if len(args) < 2:
		print 'Error: rollback requires minimum 2 arguments'
		return
	schema_dir = args[0]
	skip = [ ]
	try:
		to_version = int(args[1])
		if len(args) > 2 and args[2] == 'skip':
			skip = [int(a) for a in args[3:]]
	except ValueError, e:
		print 'Error: version number must be integer'
		return
		
	servers = config['servers']
	migrate(servers, schema_dir, rollback=True, to_version=to_version, skip=skip)

@WantArgs(1)
def action_dump(options, args, config):
	if not options.database in config['servers']:
		print "Error: no database %s in config file" % options.database
		return
	
	section = options.database
	db = section.split('.')[1]
	
	con = pymssql.connect(database=db, **config['servers'][options.database])
	cur = con.cursor()
	
	#~ TODO: какое-то тупое дублирование кода, подумать
	
	procs = dump_routines(cur, 'procedure')
	dir = args[0] + os.sep + PROC_DIR
	if not os.path.exists(dir):
		os.makedirs(dir)
	for proc in procs:
		save_routine(dir, proc)
	
	funcs = dump_routines(cur, 'function')
	dir = args[0] + os.sep + FUNC_DIR
	if not os.path.exists(dir):
		os.makedirs(dir)
	for func in funcs:
		save_routine(dir, func)

	con.close()

@WantArgs(0)
def action_doc(options, args, config):
	print help('sqlup')

def main():
	"""
	Разбирает параметры командной строки, читает конфиг и вызывает функцию action_ACTION,
	где ACTION - первый позиционный параметр
	"""
	
	parser = OptionParser(
		usage="usage: %prog [OPTIONS] action schema_directory\npossible actions: migrate, rollback, dump, doc",
		version=__version__)
	parser.add_option('-c', '--conf', dest='config', default='sqlup.conf', help='config file to use, default is "%default"')
	parser.add_option('-d', '--database', dest='database', help='database name, used with action "dump"')
	parser.add_option('-i', '--ignore-collision', action='store_true', dest='ignore', default=False, help='ignore database collisions')
	(options, args) = parser.parse_args()
	config = get_config(options.config)

	actions = {
		'migrate': action_migrate,
		'rollback': action_rollback,
		'dump': action_dump,
		'doc': action_doc,
	}
	if not config or len(args) == 0 or not args[0] in actions:
		parser.print_help()
		sys.exit()
	actions[args[0]](options, args[1:], config)

if __name__ == '__main__':
	main()
