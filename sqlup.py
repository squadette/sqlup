#!/usr/bin/env python
# -*- coding: utf-8 -*-
__version__ = "$Id$"

import sys, os, re, traceback, pymssql, ConfigParser
from pymssql import DatabaseError
from datetime import datetime
from optparse import OptionParser
from cStringIO import StringIO
import logging as log

LOGFORMAT = '%(message)s'

PROC_DIR = 'procedures'
FUNC_DIR = 'functions'
MIGR_DIR = 'migration'
TABLE_DIR = 'tables'
SQLUP_CUT='-- SQLUP-CUT'


def print_exception():
	if log.getLevelName(log.getLogger().level) == 'DEBUG':
		out = StringIO()
		traceback.print_exception(sys.exc_type, sys.exc_value, sys.exc_traceback, None, out)
		ret = out.getvalue()
		out.close()
		log.debug('\nException raised. ' + ret)
	else:
		log.error('\n%s: %s\n\t(use --verbose option to see full stacktrace)\n' % (sys.exc_type.__name__, sys.exc_value))


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
		log.error("Config file error: %s" % e)
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

def dump_indexes(cur, table_info):
	"""
	Вытаскивает индексы к таблице table
	"""
	ret = []
	query = "EXEC sp_helpindex %s"
	cur.execute(query, (table_info['name'],))
	for row in cur.fetchall():
		flags = re.sub(r' located on (.*)', '', row[1]).split(', ')
		location = re.findall(r'located on (.*)', row[1])[0]
		index_info = {
			'name' : row[0],
			'columns' : row[2].split(', '),
			'column_list' : row[2],
			'flags' : flags,
			'location' : location
		}
		if "primary key" in index_info['flags'] and len(index_info['columns']) == 1:
			for column in table_info['columns']:
				if column['COLUMN_NAME'] == index_info['columns'][0]:
					column['primary'] = index_info
		else:
			ret.append(index_info)
	table_info['indexes'] = ret

def dump_constraints(cur, table_info):
	ret = []
	query = "select CCU.table_name src_table, CCU.constraint_name src_constraint, CCU.column_name src_col, KCU.table_name target_table, KCU.column_name target_col from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU, INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC, INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU where CCU.constraint_name = RC.constraint_name and KCU.constraint_name = RC.unique_constraint_name AND CCU.table_name = '%s' order by CCU.constraint_name" % table_info['name']
	cur.execute(query)
	for row in cur.fetchall():
		for column in table_info['columns']:
			if column['COLUMN_NAME'] == row[2]:
				column['foreign_key'] = {
					'name' : row[1],
					'target_table' : row[3],
					'target_column' : row[4],
					} 
	

def dump_tables(cur):
	ret = []
	query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_schema = 'dbo' and table_type = 'BASE TABLE'"
	cur.execute(query)
	column_fields = (
		'TABLE_CATALOG',
		'TABLE_SCHEMA',
		'TABLE_NAME',
		'COLUMN_NAME',
		'ORDINAL_POSITION',
		'COLUMN_DEFAULT',
		'IS_NULLABLE',
		'DATA_TYPE',
		'CHARACTER_MAXIMUM_LENGTH',
		'CHARACTER_OCTET_LENGTH',
		'NUMERIC_PRECISION',
		'NUMERIC_PRECISION_RADIX',
		'NUMERIC_SCALE',
		'DATETIME_PRECISION',
		'CHARACTER_SET_CATALOG',
		'CHARACTER_SET_SCHEMA',
		'CHARACTER_SET_NAME',
		'COLLATION_CATALOG',
		'COLLATION_SCHEMA',
		'COLLATION_NAME',
		'DOMAIN_CATALOG',
		'DOMAIN_SCHEMA',
		'DOMAIN_NAME',
		)
	for table in cur.fetchall():
		t = table[0]
		t_columns = []
		query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'dbo' AND table_name = %s ORDER BY ordinal_position ASC"
		cur.execute(query, (t,))
		for col in cur.fetchall():
			column = {}
			for i in range(len(column_fields)):
				column[column_fields[i]] = col[i]
			t_columns.append(column)
		
		table_info = {
			'name': t,
			'columns': t_columns,
		}
		dump_indexes(cur, table_info)
		dump_constraints(cur, table_info)
		ret.append(table_info)
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
	
	log.debug('%s loaded' % name)
	return definition

def save_routine(dir, proc):
	"""
	Сохраняет код процедуры или функции в файле в указанной директории
	"""
	fname = dir + os.sep + proc['name'] + '.sql'
	log.info('writing %s' % fname)
	f = open(fname, 'w')
	f.write(proc['definition'])
	f.close()


def save_table(dir, table):
	"""
	Сохраняет DDL таблицы в файл в указанной директории
	"""
	definition = 'CREATE TABLE [dbo].[%s] (\n\t' % table['name']
	cols_def = []
	for col in table['columns']:
		col['size'] = ''
		#for s in ('CHARACTER_MAXIMUM_LENGTH', 'NUMERIC_PRECISION', 'DATETIME_PRECISION'):
		for s in ('CHARACTER_MAXIMUM_LENGTH',):
			if col[s]: col['size'] = '(%s)' % col[s]
		col['collation'] = ''
		if col['COLLATION_NAME']: col['collation'] = 'COLLATE ' + col['COLLATION_NAME']
		col['nullable'] = 'NULL'
		if col['IS_NULLABLE'] == 'NO': col['nullable'] = 'NOT NULL'
		col['default'] = ''
		if col['COLUMN_DEFAULT']:
			col['default'] = "'%s'" % col['COLUMN_DEFAULT']
		if col.has_key('primary') and col['primary']:
			primary = "PRIMARY KEY"
			if "clustered" in col['primary']['flags']:
				primary += " CLUSTERED"
			else:
				primary += " NONCLUSTERED"
			col['primary'] = primary
		else:
			col['primary'] = ""
		if col.has_key('foreign_key') and col['foreign_key']:
			fk = "FOREIGN KEY REFERENCES %(target_table)s (%(target_column)s)" % col['foreign_key']
			col['foreign_key'] = fk
		else:
			col['foreign_key'] = ""
		#print col
		str = '[%(COLUMN_NAME)s] [%(DATA_TYPE)s]%(size)s %(collation)s %(default)s %(nullable)s %(primary)s %(foreign_key)s' % col
		cols_def.append(str)
	
	definition += ',\n\t'.join(cols_def)
	definition += '\n);\n'
	
	for index in table['indexes']:
		if "unique" in index['flags']:
			index["unique"] = "UNIQUE"
		else:
			index["unique"] = ""
		if "clustered" in index['flags']:
			index["clustered"] = "CLUSTERED"
		else:
			index["clustered"] = "NONCLUSTERED"
		index["table_name"] = table['name']
		
		definition += "CREATE %(unique)s %(clustered)s INDEX [%(name)s] ON [%(table_name)s] (%(column_list)s);\n" % index


	log.debug('definition for table %s:\n%s' % (table['name'], definition))

	fname = dir + os.sep + table['name'] + '.sql'
	log.info('writing %s' % fname)
	f = open(fname, 'w')
	f.write(definition)
	f.close()


def migrate(servers, schema_dir, rollback=False, ignore=False, to_version=None, skip=[]):
	"""
	Выполняет миграцию схемы из данной директории на сервера из списка
	"""
	
	log.info('migrating...')
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
			log.info('Database schema info: version %i, last update %s' %
				 (db_version, last_update.strftime('%Y-%m-%d %H:%M')))
			update_needed = (rollback and to_version < db_version) or (not rollback and to_version > db_version)
			
			tcoll = find_table_collision(cur)
			rcoll = find_routine_collision(cur)
			if tcoll:
				log.error('Table collision(s) detected:\n')
				for coll in tcoll:
					log.error('\ttable %s is altered at %s\n' % coll)
				if ignore:
					log.info("Ignoring collisions.")
				else:
					log.info('\nTo migrate schema while collision detected, use option "--ignore"')

			if ignore or skip or not (tcoll or rcoll):
				if update_needed:
					log.info('Migrating database schema to version %i' % to_version)
					if rollback:
						mig_scripts = refine_scripts(scripts[MIGR_DIR], to_version + 1, db_version, skip)
						field = 'sqldown'
					else:
						mig_scripts = refine_scripts(scripts[MIGR_DIR], db_version + 1, to_version, skip)
						field = 'sqlup'
					log.info('Running migrations scripts...')
					run_scripts(mig_scripts, field, cur)
					log.info('Migrations scripts done')
					log.info('Updating schema_info table')
					query = 'update schema_info set schema_version = %i, last_update = getdate()' % to_version
					cur.execute(query)
				else:
					if to_version is None:
						log.warning('No migration scripts provided')
					else:
						log.info('Database schema version %i, no need to update/rollback to version %i' % (db_version, to_version))

			if rcoll:
				log.error('Routine collision(s) detected:\n')
				for coll in rcoll:
					log.error('\troutine %s is altered at %s\n' % coll)
				if ignore:
					log.info("Ignoring collisions.")
				else:
					log.info('\nTo migrate schema while collision detected, use options "--ignore"')
			if ignore or not (tcoll or rcoll):
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
		log.error("Error occured while reading schema_info table: %s" % e)
		sys.exit(1)
		
	return (db_version, last_update)


def update_routines(scripts, cursor):
	"""
	Обновляет хранимые процедуры и функции в текущей БД
	"""
	log.info('Updating routines...')
	for script in scripts:
		proc_name = os.path.splitext(script['script'])[0]
		query = 'SELECT specific_name FROM INFORMATION_SCHEMA.ROUTINES where specific_name = %s'
		cursor.execute(query, (proc_name,))
		if cursor.rowcount > 0:
			log.info('\t' + proc_name)
			cursor.execute(script['sql'])
		else:
			raise Exception('Procedure %s does not exist' % proc_name)
	log.info('Routines update done.')
	log.info('Updating schema_info.last_update')
	query = 'update schema_info set last_update = getdate()'
	cursor.execute(query)


def run_scripts(scripts, field, cursor):
	"""
	Выполняет в базе код из поля field массива scripts
	"""
	for script in scripts:
		log.info('\t' + script['script'])
		cursor.execute('BEGIN TRAN')
		s = '\n'.join(script[field].splitlines())
		batches = s.split('\nGO\n')
		for sql in batches:
			log.debug('Running query: "%s"' % sql)
			cursor.execute(sql)
		cursor.execute('COMMIT')


def extract_version(file):
	"""
	Выдирает начальные цифры из имени файла и склеивает их в INT
	"""
	
	n = re.compile('^(\d+).*').match(file)
	if not n:
		log.error("Error: incorrect file name: %s" % file)
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


def validate_schema_dir(config, dir):
	valid = True
	for server, db in [s.split('.') for s in config['servers']]:
		server_dir = os.path.join(dir, server)
		if not os.path.isdir(server_dir):
			valid = False
			log.error('Error: directory for server "%s" (listed in config file) not found in schema directory %s' % (server, dir))
		db_dir = os.path.join(server_dir, db)
		if not os.path.isdir(db_dir):
			valid = False
			log.error('Error: directory for db "%s" (listed in config file) not found in server directory %s' % (db, server_dir))
		for d in (PROC_DIR, FUNC_DIR, MIGR_DIR):
			subdir = os.path.join(db_dir, d)
			if not os.path.isdir(subdir):
				valid = False
				log.error('Error: necessary directory "%s" not found in db directory %s' % (subdir, db_dir))
	return valid


def action_migrate(options, args, config):
	if len(args) < 1:
		log.error('Error: migrate requires minimum 1 argument - schema directory')
		return
	schema_dir = args[0]
	if not validate_schema_dir(config, schema_dir):
		log.error('Error: invalid schema directory structure')
		return
	servers = config['servers']
	skip = [ ]
	try:
		if len(args) > 1 and args[1] == 'skip':
			skip = [int(a) for a in args[2:]]
	except ValueError, e:
		log.error('Error: version number must be integer')
		return
		
	migrate(servers, schema_dir, ignore=options.ignore, skip=skip)


def action_rollback(options, args, config):
	if len(args) < 2:
		log.error('Error: rollback requires minimum 2 arguments - schema directory and migration number')
		return
	schema_dir = args[0]
	skip = [ ]
	try:
		to_version = int(args[1])
		if len(args) > 2 and args[2] == 'skip':
			skip = [int(a) for a in args[3:]]
	except ValueError, e:
		log.error('Error: version number must be integer')
		return
		
	servers = config['servers']
	migrate(servers, schema_dir, ignore=options.ignore, rollback=True, to_version=to_version, skip=skip)


def action_dump(options, args, config):
	if not options.database in config['servers']:
		log.error("Error: no database specified. Try 'sqlup.py --help' for options list.")
		return
	if not options.database in config['servers']:
		log.error("Error: no database %s in config file" % options.database)
		return
	if len(args) == 0:
		log.error("Error: please, specify destination directory" % options.database)
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

	tables = dump_tables(cur)
	dir = args[0] + os.sep + TABLE_DIR
	if not os.path.exists(dir):
		os.makedirs(dir)
	for t in tables:
		save_table(dir, t)

	con.close()


def action_doc(options, args, config):
	print help('sqlup')


def startup_error(str):
	log.error("Startup error: %s. Try --verbose option for more info or --help for options list." % str)
	sys.exit()
	

def main():
	"""
	Разбирает параметры командной строки, читает конфиг и вызывает функцию action_ACTION,
	где ACTION - первый позиционный параметр
	"""

	usage = """%prog [OPTIONS] action [schema_directory]
Possible actions: migrate, rollback, dump, doc
Samples:
\t%prog -c sqlup.conf migrate c:/myproject/schema_dir
\t%prog -i migrate c:/myproject/schema_dir (ignoring collisions and using config file from current directory)
\t%prog -c sqlup.conf -d server1.my_db ./dump_dir"""
	parser = OptionParser(usage=usage, version=__version__)
	parser.add_option('-c', '--conf', dest='config', default='sqlup.conf', help='config file to use, default is %default in current directory')
	parser.add_option('-d', '--database', dest='database', help='database name, used with action "dump"')
	parser.add_option('-i', '--ignore-collision', action='store_true', dest='ignore', default=False, help='ignore database collisions')
	parser.add_option('-v', '--verbose', action='store_true', dest='verbose', default=False, help='verbose mode')
	(options, args) = parser.parse_args()
	level = log.INFO
	if options.verbose:
		level=log.DEBUG
	log.basicConfig(format=LOGFORMAT, level=level, stream=sys.stdout)

	log.debug('Loading configuration from file %s ...' % options.config)
	config = get_config(options.config)
	error = False

	actions = {
		'migrate': action_migrate,
		'rollback': action_rollback,
		'dump': action_dump,
		'doc': action_doc,
	}

	if not config or len(args) == 0 or not args[0] in actions:
		error = True

	if error:
		if not config:
			startup_error("cannot open config file %s" % options.config)
		if len(args) == 0:
			startup_error("no action specified")
		elif not args[0] in actions:
			startup_error("\"%s\" is not a valid action" % args[0])

	try:
		actions[args[0]](options, args[1:], config)
	except Exception:
		print_exception()

if __name__ == '__main__':
	main()
