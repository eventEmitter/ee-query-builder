!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, type 			= require('ee-types')
		, moment 		= require('moment')
		, Functions 	= require('./Functions');



	module.exports = new Class({


		_operators: {
			  '=': 	'='
			, '<': 	'<'
			, '>': 	'>'
			, '>=': '>='
			, '<=': '<='
			, '!=': '!='
			, 'equal': '='
			, 'notEqual': '!='
			, 'lt': '<'
			, 'gt': '>'
			, 'gte': '>='
			, 'lte': '<='
			, 'not': 'is not'
			, 'is': 'is'
		}



		/**
		 * class constructor
		 *
		 * @param <Object> connection options
		 */
		, init: function init(options) {
			this._escapeId 	= options.escapeId;
			this._escape 	= options.escape;
			this._type 		= options.type;

			options.queryBuilder = this;

			this._functions = new Functions(options);
		}



		/**
		 * the _render() method creates an sql query from an object
		 *
		 * @param <Object> query
		 */
		, _render: function(mode, query, parameters){

			// maybe we get already some parmeters from a parent query ..
			parameters = parameters || {};

			// if dont get a query ...
			query = query || {};


			switch (mode) {
				case 'query':
					return this._buildSelectQuery(query, parameters);

				case 'update':
					return this._buildUpdateQuery(query, parameters);

				case 'insert':
					return this._buildInsertQuery(query, parameters);

				case 'delete':
					return this._buildDeleteQuery(query, parameters);


				default:
					throw new Error('Unknown query mode «'+mode+'»!').setName('InvalidModeException');
			}
		}



		, _buildDeleteQuery: function(query, parameters) {
			var SQLString = 'DELETE FROM '
				, keys = []
				, values = [];

			// from
			SQLString += this._renderFrom(query.database, query.from);


			// we need to render a subquery for every selected field (primary key)
			SQLString += ' WHERE ' + query.select.map(function(column) {
				return this._escapeId(query.from)+'.'+this._escapeId(column)+' IN('+this._render('query', {
					  select 	: [column]
					, from 		: query.from
					, database 	: query.database
					, filter 	: query.filter
					, join 		: query.join
					, order 	: query.order
					, group 	: query.group
					, limit 	: query.limit
					, offset 	: query.offset
				}, parameters).SQLString+')'
			}.bind(this)).join(' AND ');
			
			return {SQLString: SQLString, parameters: parameters};
		}



		, _buildInsertQuery: function(query, parameters) {
			var SQLString = 'INSERT INTO '
				, keys = []
				, values = [];

			// from
			SQLString += this._renderFrom(query.database, query.from);

			Object.keys(query.values).forEach(function(key){				
				var id = this._getParameterName(parameters, key);
				parameters[id] = query.values[key];

				keys.push(this._escapeId(key));
				values.push('?'+id);
			}.bind(this));

			if (keys.length) SQLString += ' ('+keys.join(', ')+') VALUES ('+values.join(', ')+')';
			else if (this._type === "postgres") SQLString += ' DEFAULT VALUES';


			if (this._type === 'postgres' && query.returning && query.returning.length) {
				SQLString += ' RETURNING '+ query.returning.map(function(key){
					return this._escapeId(key);
				}.bind(this)).join(', ');
			}
			
			return {SQLString: SQLString, parameters: parameters};
		}


		

		, _buildUpdateQuery: function(query, parameters) {
			var SQLString = 'UPDATE '
				, updates = [];

			// from
			SQLString += this._renderFrom(query.database, query.from);

			SQLString += ' SET ';
			Object.keys(query.values).forEach(function(key) {
				updates.push(this._renderValue(parameters, key, query.values[key]));
			}.bind(this));

			SQLString += updates.join(', ') +' ';


			// we need to render a subquery for every selected field (primary key)
			SQLString += ' WHERE ' + query.select.map(function(column) {
				return this._escapeId(query.from)+'.'+this._escapeId(column)+' IN('+this._render('query', {
					  select 	: [column]
					, from 		: query.from
					, database 	: query.database
					, filter 	: query.filter
					, join 		: query.join
					, order 	: query.order
					, group 	: query.group
					, limit 	: query.limit
					, offset 	: query.offset
				}, parameters).SQLString+')'
			}.bind(this)).join(' AND ');

			return {SQLString: SQLString, parameters: parameters};
		}

		/*
		 * render a value used in a nupdate query
		 */
		, _renderValue: function(parameters, filedName, value) {
			var   id = this._getParameterName(parameters, filedName)
				, fn;

			if (type.function(value)) {
				fn = value();

				switch(fn.fn) {
					case 'increaseBy':
						parameters[id] = fn.value;
						return this._escapeId(filedName)+' = '+this._escapeId(filedName)+' + ?'+id;

					case 'decreaseBy':
						parameters[id] = fn.value;
						return this._escapeId(filedName)+' = '+this._escapeId(filedName)+' - ?'+id;

					default:
						throw new Error('Failed to identify the value for the field «'+filedName+'»! unknown function «'+fn.fn+'» provided!');
				};

			}
			else {
				parameters[id] = value;
				return this._escapeId(filedName)+' = ?'+id;
			}
		}



		, _buildSelectQuery: function(query, parameters){
			var SQLString = ''
				, filter;

			// render select			
			SQLString += ' SELECT ' + (this._renderSelect(parameters, (query.select || []), query.from) || '*');

			// from
			SQLString += ' FROM ' + this._renderFrom(query.database, query.from);

			// joins
			if (query.join && query.join.length) SQLString += this._renderJoin(query.database, query.join);

			// render filter (where statement)
			if (query.filter && Object.keys(query.filter).length) filter = this._renderFilter(parameters, query.filter || {});
			if (filter && filter.length && filter.trim().length) SQLString += ' WHERE ' + filter;

			// render order statement
			if(query.order && query.order.length) SQLString += ' ORDER BY ' + (this._renderOrder(query.order || []) || 1);

			// render group statement
			if(query.group && query.group.length) SQLString += ' GROUP BY ' + (this._renderGroup(query.group ) || 1);


			// limit & offset
			if(query.limit && !isNaN(parseInt(query.limit, 10))) SQLString += ' LIMIT ' + parseInt(query.limit, 10);
			if(query.offset && !isNaN(parseInt(query.offset, 10))) SQLString += ' OFFSET ' + parseInt(query.offset, 10);


			return {SQLString: SQLString, parameters: parameters};
		}




		, _renderGroup: function(group) {
			return group.map(function(grouping){
				return this._escapeId(grouping.table || '') + '.' + this._escapeId(grouping.column || '');
			}.bind(this)).join(', ');
		}




		, _renderJoin: function(database, joins){
			var SQLString = '';

			joins.forEach(function(join){				
				switch (join.type) {
					case 'inner':
						SQLString += ' INNER JOIN ';
						break;

					case 'left':
						SQLString += ' LEFT JOIN ';
						break;

					default: 
						throw new Error('Unknown join type «'+join.type+'»!').setName('InvalidJoinTypeException');

				}


				SQLString += this._escapeId(database) + '.' + this._escapeId(join.target.table) + (join.alias ? ' as '+this._escapeId(join.alias) : '') +' ON ';
				SQLString += this._escapeId(join.source.table) +'.'+this._escapeId(join.source.column)+'=';
				SQLString += this._escapeId(join.alias || join.target.table) +'.'+this._escapeId(join.target.column);
			}.bind(this));

			return SQLString;
		}


		, _renderFrom: function(database, table) {
			return this._escapeId(database || 'undefined') + '.' + this._escapeId(table || 'undefined');
		}


		, _renderOrder: function(order) {
			var instructions = [];

			order.sort(function(a, b) {
				return type.object(a) && type.object(b) ? (a.priority - b.priority) : 0;
			});

			order.forEach(function(instruction){
				switch (type(instruction)) {
					case 'string':
						instructions.push(this._escapeId(instruction));
						break;

					case 'object':
						instructions.push(this._escapeId(instruction.entity)+'.'+this._escapeId(instruction.property) + (instruction.desc ? ' DESC' : ' ASC'));
						break;

					default:
						throw new Error('Invalid type «'+type(instruction)+'» for order statement!').setName('InvalidOrderException');
				}
			}.bind(this));


			return instructions.length ? instructions.join(', ') : '1';
		}


		/**
		 * the _renderFilter() method creates an sql where statement from 
		 *
		 * @param <Object> query parameters
		 * @param <Object> select tree
		 */
		, _renderSelect: function(parameters, select, tablename) {
			var selects = []
				, command;

			// remove unused selects
			if (select.indexOf('*') >= 0) {
				select = select.filter(function(item){
					return item === '*' || !type.string(item);
				}.bind(this));
			}

			if (!select.length) selects.push(this._escapeId(tablename)+'.*'); 

			select.forEach(function(selector){
				switch (type(selector)) {
					case 'string':
						selects.push(this._escapeId(tablename)+'.'+(selector === '*' ? '*' : this._escapeId(selector)));
						break;

					// subquery
					case 'object':
						selects.push('('+this._render('query', selector.query, parameters).SQLString+') as '+this._escapeId(selector.alias || 'undefined'));
						break;

					// functions
					case 'function':
						command = selector();
						if (command.table){
							selects.push(this._escapeId(command.table)+'.'+this._escapeId(command.column)+(command.alias ? ' as '+ command.alias :''));
						}
						else if (command.fn) {
							selects.push(this._functions.renderSelectFunction(tablename, command));
						}
						else if (command.keyWord) {
							selects.push(command.keyWord);
						}
						break;
				}
			}.bind(this));

			return selects.join(', ');
		}


		/**
		 * the _renderFilter() method creates an sql where statement from 
		 *
		 * @param <Object> query parameters
		 * @param <Object> filter tree
		 * @param <String> name of the current property
		 * @param <String> name of the current entity
		 */
		, _renderFilter: function(parameters, filter, property, entity){
			var   items = []
				, result
				, id;

			switch (type(filter)) {
				case 'array':
					filter.forEach(function(filterItem) {
						result = this._renderFilter(parameters, filterItem, (property === '_' ? (entity || property) : (property || entity)), entity);
						if (result !== undefined) items.push(result);
					}.bind(this));
					return items.length ? ( items.length === 1 ? items[0] : '(' + items.join(filter.mode === 'and' ? ' AND ' : ' OR ') + ')' ) : '';


				case 'object':
					if (type.function(filter.isQuery) && filter.isQuery()) {

						// returns it
						return this._escapeId(entity)+'.'+this._escapeId(property)+' = ('+this._render('query', this._prepareQueryFilter(filter), parameters).SQLString+')';
					}
					else {
						Object.keys(filter).forEach(function(name) {
							result = this._renderFilter(parameters, filter[name], name, property)
							if (result !== undefined) items.push(result);
						}.bind(this));
						return items.length ? ( items.length === 1 ? items[0] : '(' + items.join(' AND ') + ')' ): undefined;
					}
					break;

				case 'string':
				case 'number':
				case 'date':
				case 'boolean':
					id = this._getParameterName(parameters, property);
					parameters[id] = filter;
					return (entity ? (this._escapeId(entity ) + '.') : '') + this._escapeId(property || '') + ' = ?'+id;


				case 'null':
				case 'undefined':
					return (entity ? (this._escapeId(entity ) + '.') : '') + this._escapeId(property || '') + ' is null';
				

				case 'function':
					return this._renderCommand(property, filter(), parameters, (entity ? (this._escapeId(entity ) + '.') : ''));


				default: 
					throw new Error('Cannot process the type «'+type(filter)+'» in the MySQL querybuilder!').setName('InvalidTypeException');
			}
		}



		/*
		 * compile a query
		 */
		, _prepareQueryFilter: function(filter){
			var resource = filter.getRootResource();

			resource.setSelectMode();
			resource.prepare(null, true);

			return resource.query
		}



		, _renderCommand: function(property, command, parameters, entity){
			var id, result;

			// comparison
			if (command.operator) {
				id = this._getParameterName(parameters, property);

				// must be a valid operator
				if (!this._operators[command.operator]) throw new Error('Unknown operator «'+command.operator+'»!').setName('InvalidOperatorError');

				// is it a subquery or is it scalar value?
				if (command.query) {
					return entity+this._escapeId(property) + ' ' + this._operators[command.operator] + ' (' + this._render('query', command.query, parameters).SQLString +')';
				}
				else if (type.function(command.value.isQuery) && command.value.isQuery()) {					
					return entity+this._escapeId(property)+' '+this._operators[command.operator]+ ' (' + this._render('query', this._prepareQueryFilter(command.value), parameters).SQLString +')';
				}
				else {
					parameters[id] = command.value;
					return entity+this._escapeId(property) + ' ' + this._operators[command.operator] + ' ?'+id;
				}
			}

			// function
			else if (command.fn) {
				if (!this._functions[command.fn]) throw new Error('Unknown function «'+command.fn+'»!').setName('InvalidOperatorError');
				result = this._functions[command.fn](command, parameters);
				if (result !== undefined) return entity+this._escapeId(property) + ' ' + this._functions[command.fn](command);
				return '';				
			}

			// unknown
			else throw new Error('Unknwon command «'+JSON.stringify(command)+'»!').setName('InvalidTypeException')
		}



		, _getParameterName: function(parameters, name){
			var i = 0;
			while(parameters[name+i]) i++;
			return name+i;
		}


		/**
		 * the _toString() converts types to db compatible string types
		 *
		 * @param <Mixed> input
		 */
		, _toString: function(input){
			switch(type(input)){
				case 'number':
					return isNaN(input) ? 'null' : ''+input;

				case 'string':
					return input;

				case 'boolean':
					return input ? '1' : '0';

				case 'null':
				case 'undefined':
					return null;

				case 'date':
					return moment(input).format('YYYY-MM-DD HH:mm:ss');

				case 'function':
					//return input();

				default:
					throw new Error('Failed to convert type «'+type(input)+'» to string!').setName('InvalidTypeException')
			}
		}


		/**
		 * the _toType() converts db string types to js types
		 *
		 * @param <String> input
		 */
		, _toType: function(input){

		}
	});
}();
