!function(){
    'use strict';


    var   Class         = require('ee-class')
        , log           = require('ee-log')
        , EventEmitter  = require('ee-event-emitter')
        , type          = require('ee-types');



    module.exports = new Class({    
        inherits: EventEmitter


        // known aggregate functions
        , aggregateFunctions: {
              avg   : 'AVG'
            , sum   : 'SUM'
            , min   : 'MIN'
            , max   : 'MAX'
            , count : 'COUNT'
        }

        
        /**
         * class constructor
         *
         * @param <Object> options
         */
        , init: function(options){
            this._escapeId      = options.escapeId;
            this._escape        = options.escape;
            this._queryBuilder  = options.queryBuilder;
        }


        /**
         * select functions
         *
         * @param <Object> options
         */
        , renderSelectFunction: function(tablename, command) {
            return this.aggregateFunctions[command.fn]+'('+this._escapeId(tablename)+'.'+this._escapeId(command.value)+')'+(command.alias ? ' AS '+this._escapeId(command.alias) : '');
        }



        /**
         * reference on other tables
         *
         * @param <Object>  instruction
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         */
        , reference: function(command, paramaters) {
            return '= '+this._escapeId(command.entity)+'.'+this._escapeId(command.column);
        }

        
        /**
         * SQL in statement
         *
         * @param <Object>  instruction
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         */
        , in: function(command, paramaters) {
            var values = command.values;

            if (type.array(command.values) && command.values.length && type.function(command.values[0].isQuery)) values = values[0];

            if (type.function(values.isQuery)) {
                return ' IN ('+this._queryBuilder._renderSubQuery(values, paramaters)+')';
            }
            else {
                if (values.length) {
                    return ' IN (' +values.map(function(value) {
                        return this._escape(value);
                    }.bind(this)).join(', ') +')';
                } else return ' false';
            }
        }



        /**
         * SQL not in statement
         *
         * @param <Object>  instruction
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         */
        , notIn: function(command, paramaters) {
            var values = command.values;
            
            if (type.array(command.values) && command.values.length && type.function(command.values[0].isQuery)) values = values[0];
            
            if (type.function(values.isQuery)) {
                return ' NOT IN ('+this._queryBuilder._renderSubQuery(values, paramaters)+')';
            }
            else {
                if (values.length) {
                    return ' NOT IN (' +values.map(function(value){
                        return this._escape(value);
                    }.bind(this)).join(', ') +')';
                } else return undefined;
            }
        }


        
        /**
         * SQL like statement
         *
         * @param <Object>  instruction
         */
        , like: function(command) {
            return ' ILIKE '+this._escape(command.value);
        }
 


        /**
         * SQL not like statement
         *
         * @param <Object>  instruction
         */
        , notLike: function(command) {
            return ' NOT ILIKE '+this._escape(command.value);
        }
        



        /**
         * SQL is null
         */
        , 'null': function(){
            return ' is null';
        }



        /**
         * SQL is not null
         */
        , 'notNull': function(){
            return ' is not null';
        }
    });
}();
