!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, type 			= require('ee-types');



	module.exports = new Class({

		init: function(options){
			this._escapeId 		= options.escapeId;
			this._escape 		= options.escape;
			this._queryBuilder 	= options.queryBuilder;
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


		, 'null': function(){
			return ' is null';
		}


		, 'notNull': function(){
			return ' is not null';
		}
	});
}();
