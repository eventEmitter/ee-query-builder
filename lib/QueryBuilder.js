!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, type 			= require('ee-types')
		, moment 		= require('moment')
		, async 		= require('ee-async');



	module.exports = new Class({


		_operators: {
			  '=': 	'='
			, '<': 	'<'
			, '>': 	'>'
			, '>=': '>='
			, '<=': '<='
			, '!=': '!='
			, 'equal': '='
			, 'notequal': '!='
			, 'lt': '<'
			, 'gt': '>'
			, 'gte': '>='
			, 'lte': '<='
			, 'not': 'is not'
			, 'is': 'is'
		}

		, _functions: {
			  'null': 'is null'
			, notNull: 'is not null'
		}


		/**
		 * class constructor
		 *
		 * @param <Object> connection options
		 */
		, init: function init(options) {
			this._escapeId 	= options.escapeId;
			this._escape 	= options.escpae;
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
					return this._buildQuery(query, parameters);

				default:
					throw new Error('Unknown query mode «'+mode+'»!').setName('InvalidModeException');
			}
		}




		, _buildQuery: function(query, parameters){
			var SQLString = '';

			// render select			
			SQLString += ' SELECT ' + (this._renderSelect(parameters, query.select || []) || 1);

			// from
			SQLString += ' FROM ' + this._renderFrom(query.from);

			// render filter (where statement)
			SQLString += ' WHERE ' + (this._renderFilter(parameters, query.filter || {}) || 1);

			// render order statement
			SQLString += ' ORDER BY ' + (this._renderOrder(query.order || []) || 1);


			// limit & offset
			if(query.offset && !isNaN(parseInt(query.offset, 10))) SQLString += ' OFFSET ' + parseInt(query.offset, 10);
			if(query.limit && !isNaN(parseInt(query.limit, 10))) SQLString += ' LIMIT ' + parseInt(query.limit, 10);

			return {SQLString: SQLString, parameters: parameters};
		}


		, _renderFrom: function(from) {
			switch (type(from)) {
				case 'string':
					return this._escapeId(from || 'undefined');

				case 'object':
					return this._escapeId(from.database || 'undefined') + '.' + this._escapeId(from.table || 'undefined');
			}
		}


		, _renderOrder: function(order) {
			var instructions = [];

			order.forEach(function(instruction){
				switch (type(instruction)) {
					case 'string':
						instructions.push(this._escapeId(instruction));
						break;

					case 'object':
						instructions.push(this._escapeId(instruction.property) + (instruction.desc ? 'DESC' : 'ASC'));
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
		, _renderSelect: function(parameters, select) {
			var selects = [];

			select.forEach(function(selector){
				switch (type(selector)) {
					case 'string':
						selects.push(this._escapeId(selector));
						break;

					// subquery
					case 'object':
						selects.push('('+this._render('query', selector.query, parameters).SQLString+') as '+this._escapeId(selector.alias || 'undefined'));
						break;

					// functions
					case 'function':

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
				, id;

			switch (type(filter)) {
				case 'array':
					filter.forEach(function(filterItem){
						items.push(this._renderFilter(parameters, filterItem, (property === '_' ? (entity || property) : (property || entity)) ));
					}.bind(this));
					return items.length ? ( items.length === 1 ? items[0] : '(' + items.join(' OR ') + ')' ) : '';


				case 'object':
					Object.keys(filter).forEach(function(name){
						items.push(this._renderFilter(parameters, filter[name], name, property));
					}.bind(this));
					return items.length ? ( items.length === 1 ? items[0] : '(' + items.join(' AND ') + ')' ): '';


				case 'string':
				case 'number':
				case 'date':
				case 'boolean':
					id = this._getParameterName(parameters, property);
					parameters[id] = filter;
					return (entity ? (this._escapeId(entity ) + '.') : '') + this._escapeId(property || '') + ' = ?'+id;


				case 'null':
				case 'undefined':
					return (entity ? (this._escapeId(entity ) + '.') : '') + this._escapeId(property || '') + ' null';
				

				case 'function':
					return this._renderCommand(property, filter(), parameters);


				default: 
					throw new Error('Cannot process the type «'+type(filter)+'» in the MySQL querybuilder!').setName('InvalidTypeException');
			}
		}



		, _renderCommand: function(property, command, parameters){
			var id;

			// comparison
			if (command.operator) {
				id = this._getParameterName(parameters, property);

				// must be a valid operator
				if (!this._operators[command.operator]) throw new Error('Unknown operator «'+command.operator+'»!').setName('InvalidOperatorError');

				// is it a subquery or is it scalar value?
				if (command.query){
					return this._escapeId(property) + ' ' + this._operators[command.operator] + ' (' + this._render('query', command.query, parameters).SQLString +')';
				}
				else {
					parameters[id] = command.value;
					return this._escapeId(property) + ' ' + this._operators[command.operator] + ' ?'+id;
				}
			}

			// function
			else if (command.fn){
				if (!this._functions[command.fn]) throw new Error('Unknown function «'+command.fn+'»!').setName('InvalidOperatorError')
				return this._escapeId(property) + ' ' + this._functions[command.fn];
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