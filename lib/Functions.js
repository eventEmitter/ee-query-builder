!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, type 			= require('ee-types');



	module.exports = new Class({

		init: function(options){
			this._escapeId 	= options.escapeId;
			this._escape 	= options.escape;
		}

		, in: function(command){
			//if (command.values.length) {
				return ' IN (' +command.values.map(function(value){
					return this._escape(value);
				}.bind(this)).join(', ') +')';
			//}
		}


		, notIn: function(command) {
			return ' NOT IN (' +command.values.map(function(value){
				return this._escape(value);
			}.bind(this)).join(', ') +')';
		}


		, 'null': function(){
			return ' is null';
		}


		, 'notNull': function(){
			return ' is not null';
		}
	});
}();
