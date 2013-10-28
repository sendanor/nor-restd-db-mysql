/* nor-sysrestd -- db module */
module.exports = function(opts) {
	opts = opts || {};

	/** Escape URL component */
	function escape_url(value) {
		// FIXME: Implement it!
		return value;
	}

	/** Escape MySQL db component */
	function escape_db(value) {
		// FIXME: Implement it!
		return '`' + value + '`';
	}

	/** Escape MySQL table component */
	function escape_table(value) {
		return escape_db(value);
	}

	/** Get current resource ref */
	function get_ref(req) {
		return req.hostref + require('url').parse(req.url).pathname.replace(/\/+$/g, '');
	}


	var DB = require('nor-db');
	var POOL = new DB.mysql.Pool(opts);

	var routes = {
		// Setup named params
		// FIXME: Fix regexes
		':params': {
			'mysql_db_name': /^[a-zA-Z0-9\.\_\-]+$/,
			'mysql_db_table': /^[a-zA-Z0-9\.\_\-]+$/,
			'mysql_db_id': /^[0-9]+$/,
		},
		'databases': {
			':mysql_db_name': {
				'tables': {
					':mysql_db_table': {
						'rows': {
							':mysql_db_id': {}
						}
					}
				}
			}
		}
	};

	/** Get all collections (databases, etc) */
	routes.GET = function(req, res) {
		var ret = {'databases':{'$ref':get_ref(req) + '/databases'}};
		['host', 'username'].forEach(function(key) {
			if(opts[key] !== undefined) {
				ret[key] = opts[key];
			}
		});
		return ret;
	};

	/** Execute SQL query and return results */
	function do_query_action(q, params) {

		/** Build action function */
		function create_query_action(q, p) {
			function action(db) {
				return db.query(''+q, p);
			}
			return action;
		}

		var _ret, _db;

		function save_db(db) {
			return _db = db;
		}

		function save_ret(ret) {
			_ret = ret;
			return _db;
		}

		function get_ret() {
			return _ret;
		}

		return POOL.getConnection().then(save_db).then(create_query_action(''+q, params)).then(save_ret).$release().then(get_ret);
	}

	/** Get all collections (SHOW DATABASES) */
	routes.databases.GET = function(req, res) {
		function format_results(ret) {
			var result = {};
			function format_row(obj) {
				var name = ''+obj.Database;
				result[name] = {
					'name': name,
					'$ref': get_ref(req) + '/' + escape_url(name)
				};
			}
			ret.shift().forEach(format_row);
			return result;
		}
		return do_query_action('SHOW DATABASES').then(format_results);
	};

	/* Create new database (CREATE DATABASE) */
	routes.databases.POST = function(req, res) {
		return "Not implemented yet.";
	};

	/** Get database information (including tables) */
	routes.databases[':mysql_db_name'].GET = function(req, res) {
		return {'name': req.params.mysql_db_name, 'tables': {'$ref':get_ref(req)+'/tables'}};
	};

	/** Delete database */
	routes.databases[':mysql_db_name'].DELETE = function(req, res) {
		return "Not implemented yet.";
	};

	/** Get only list of tables (SHOW TABLES) */
	routes.databases[':mysql_db_name'].tables.GET = function(req, res) {
		function format_results(ret) {
			var result = {};
			function format_row(obj) {
				var name = obj['Tables_in_'+req.params.mysql_db_name];
				result[name] = {
					'name': name,
					'$ref': get_ref(req) + '/' + escape_url(name)
				};
			}
			ret.shift().forEach(format_row);
			return result;
		}
		return do_query_action('SHOW TABLES FROM ' + escape_db(req.params.mysql_db_name) ).then(format_results);
	};

	/** Create new table in the database (CREATE TABLE) */
	routes.databases[':mysql_db_name'].tables.POST = function(req, res) {
		return "Not implemented yet.";
	};

	/** Get database table rows */
	routes.databases[':mysql_db_name'].tables[':mysql_db_table'].GET = function(req, res) {
		var name = ''+req.params.mysql_db_name;
		var table = ''+req.params.mysql_db_table;
		
		return {'name': table, 'rows': {'$ref':get_ref(req)+'/rows'}};
	};

	/** Delete database table (DROP TABLE) */
	routes.databases[':mysql_db_name'].tables[':mysql_db_table'].DELETE = function(req, res) {
		return "Not implemented yet.";
	};

	/** Get database table rows */
	routes.databases[':mysql_db_name'].tables[':mysql_db_table'].rows.GET = function(req, res) {
		var name = ''+req.params.mysql_db_name;
		var table = ''+req.params.mysql_db_table;

		function format_results(ret) {
			function format_row(obj) {
				obj.$ref = get_ref(req) + '/' + obj.id;
				return obj;
			}
			return ret.shift().map(format_row);
		}

		return do_query_action('SELECT * FROM '+escape_db(name)+'.'+escape_table(table)).then(format_results);
	};

	/** Delete database table (DELETE FROM) */
	routes.databases[':mysql_db_name'].tables[':mysql_db_table'].rows.DELETE = function(req, res) {
		return "Not implemented yet.";
	};

	/** Insert new row in the table (INSERT) */
	routes.databases[':mysql_db_name'].tables[':mysql_db_table'].rows.POST = function(req, res) {
		return "Not implemented yet.";
	};

	/** Get single row by primary key (SELECT WHERE id=:id) */
	routes.databases[':mysql_db_name'].tables[':mysql_db_table'].rows[':mysql_db_id'].GET = function(req, res) {
		var name = ''+req.params.mysql_db_name;
		var table = ''+req.params.mysql_db_table;
		var id = ''+req.params.mysql_db_id;

		function format_results(ret) {
			var obj = ret.shift().shift();
			obj.$ref = get_ref(req);
			return obj;
		}

		return do_query_action('SELECT * FROM '+escape_db(name)+'.'+escape_table(table) + ' WHERE id = ?', id).then(format_results);
	};

	/** Delete single row by primary key (DELETE WHERE id=:id) */
	routes.databases[':mysql_db_name'].tables[':mysql_db_table'].rows[':mysql_db_id'].DELETE = function(req, res) {
		return "Not implemented yet.";
	};

	/** Update single row by primary key (UPDATE WHERE id=:id) */
	routes.databases[':mysql_db_name'].tables[':mysql_db_table'].rows[':mysql_db_id'].POST = function(req, res) {
		return "Not implemented yet.";
	};

	return routes;
};

/* EOF */
