# -*- coding: utf-8 -*-
__version__ = "$Id$"

import sys, os, re, pymssql, ConfigParser
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

def dump_routines(cur, type):
	"""
	Выбирает из information_schema.routines объекты указанного типа, возвращает массивом
	(выбираются только объекты с routine_body = 'SQL')
	"""
	ret = []
	query = "SELECT specific_name, CAST(routine_definition AS text), last_altered FROM INFORMATION_SCHEMA.ROUTINES where routine_body = 'SQL' AND routine_type = %s"
	cur.execute(query, (type,))
	for proc in cur.fetchall():
		ret.append({
			'name': proc[0],
			'definition': proc[1],
			'last_altered': proc[2],
		})
	return ret

def save_routine(dir, proc):
	"""
	Сохраняет код процедуры или функции в файле в указанной директории
	"""
	fname = dir + os.sep + proc['name'] + '.sql'
	print 'writing %s' % fname
	f = open(fname, 'w')
	f.write(proc['definition'])
	f.close()

def migrate(servers, schema_dir, rollback=False, to_version=None, skip=[]):
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
				to_version = extract_version(scripts[MIGR_DIR][-1]['script'])
			
			conf = servers[server]
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

			if tcoll and not skip:
				print 'To migrate schema while collision detected, use option "skip"'
			else:
				if update_needed:
					print 'Migrating database schema to version %i' % to_version
					if rollback:
						mig_scripts = refine_scripts(scripts[MIGR_DIR], to_version, db_version, skip)
						field = 'sqldown'
					else:
						mig_scripts = refine_scripts(scripts[MIGR_DIR], db_version, to_version, skip)
						field = 'sqlup'
					run_scripts(mig_scripts, field, cur)
					print 'Updating schema_info table'
					query = 'update schema_info set schema_version = %i, last_update = getdate()' % to_version
					cur.execute(query)
				else:
					print 'Database schema version %i, no need to update/rollback to version %i' % (db_version, to_version)

			rcoll = find_routine_collision(cur)
			if not len(rcoll):
				update_routines(scripts[PROC_DIR] + scripts[FUNC_DIR], cur)
			else:
				for coll in rcoll:
					print "Collistion detected: stored procedure %s was altered after last schema update" % coll[0]

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
	
	query = 'select * from schema_info'
	cursor.execute(query)
	(db_version, last_update) = cursor.fetchone()
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
			print '\tdropping routine %s' % proc_name
			query = 'drop %s %s' % (script['type'], proc_name)
			cursor.execute(query)
		print '\tcreating routine %s' % proc_name
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
		cursor.execute(script[field])

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

def action_migrate(options, args, config):
	if len(args) < 1:
		print 'Error: migrate requires minimum 1 argument'
		return
	schema_dir = args[0]
	servers = config['servers']
	skip = [ ]
	try:
		if len(args) > 1 and args[1] == 'skip':
			skip = [int(a) for a in args[2:]]
	except ValueError, e:
		print 'Error: version number must be integer'
		return
		
	migrate(servers, schema_dir, skip=skip)

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
	con = pymssql.connect(database=options.database, **config['servers'][options.database])
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
	(options, args) = parser.parse_args()
	config = get_config(options.config)

	actions = {
		'migrate': action_migrate,
		'rollback': action_rollback,
		'dump': action_dump,
		'doc': action_doc,
	}
	if len(args) == 0 or not args[0] in actions:
		parser.print_help()
		sys.exit()
	actions[args[0]](options, args[1:], config)

if __name__ == '__main__':
	main()
