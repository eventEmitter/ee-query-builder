!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, type 			= require('ee-types');



	module.exports = new Class({	
		
		aggregateFunctions: {
			  avg	: 'AVG'
			, sum	: 'SUM'
			, min	: 'MIN'
			, max	: 'MAX'
			, count	: 'COUNT'
		}

		
		, init: function(options){
			this._escapeId 		= options.escapeId;
			this._escape 		= options.escape;
			this._queryBuilder 	= options.queryBuilder;
		}


		, renderSelectFunction: function(tablename, command) {
			return this.aggregateFunctions[command.fn]+'('+this._escapeId(tablename)+'.'+this._escapeId(command.value)+')'+(command.alias ? ' AS '+this._escapeId(command.alias) : '');
		}


		, in: function(command, paramaters){
			//if (command.values.length) {
				if (type.function(command.values.isQuery)) {
					return ' IN ('+this._queryBuilder._render('query', command.values.getResource().query, paramaters).SQLString+')';
				}
				else {
					return ' IN (' +command.values.map(function(value) {
						return this._escape(value);
					}.bind(this)).join(', ') +')';
				}
			//}
		}


		, notIn: function(command, paramaters) {
			if (type.function(command.values.isQuery)) {
				return ' NOT IN ('+this._queryBuilder._render('query', command.values.getResource().query, paramaters).SQLString+')';
			}
			else {
				return ' NOT IN (' +command.values.map(function(value){
					return this._escape(value);
				}.bind(this)).join(', ') +')';
			}
		}

		

		, like: function(command) {
			return ' LIKE '+this._escape(command.value);
		}

		, notLike: function(command) {
			return ' NOT LIKE '+this._escape(command.value);
		}
		


		, 'null': function(){
			return ' is null';
		}


		, 'notNull': function(){
			return ' is not null';
		}
	});
}();
