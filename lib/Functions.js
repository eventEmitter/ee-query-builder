!function(){

    var   Class         = require('ee-class')
        , log           = require('ee-log')
        , type          = require('ee-types');



    module.exports = new Class({    
        
        aggregateFunctions: {
              avg   : 'AVG'
            , sum   : 'SUM'
            , min   : 'MIN'
            , max   : 'MAX'
            , count : 'COUNT'
        }

        
        , init: function(options){
            this._escapeId      = options.escapeId;
            this._escape        = options.escape;
            this._type          = options.type;
            this._queryBuilder  = options.queryBuilder;
        }


        , renderSelectFunction: function(tablename, command) {
            return this.aggregateFunctions[command.fn]+'('+this._escapeId(tablename)+'.'+this._escapeId(command.value)+')'+(command.alias ? ' AS '+this._escapeId(command.alias) : '');
        }


        
        , in: function(command, paramaters) {
            var values = command.values;

            if (type.array(command.values) && command.values.length && type.function(command.values[0].isQuery)) values = values[0];

            //if (command.values.length) {
                if (type.function(values.isQuery)) {
                    return ' IN ('+this._queryBuilder._render('query', this._queryBuilder._prepareQueryFilter(values), paramaters).SQLString+')';
                }
                else {
                    if (values.length) {
                        return ' IN (' +values.map(function(value) {
                            return this._escape(value);
                        }.bind(this)).join(', ') +')';
                    } else return undefined;
                }
            //}
        }


        , notIn: function(command, paramaters) {
            var values = command.values;
            
            if (type.array(command.values) && command.values.length && type.function(command.values[0].isQuery)) values = values[0];
            
            if (type.function(values.isQuery)) {
                return ' NOT IN ('+this._queryBuilder._render('query', this._queryBuilder._prepareQueryFilter(values), paramaters).SQLString+')';
            }
            else {
                if (values.length) {
                    return ' NOT IN (' +values.map(function(value){
                        return this._escape(value);
                    }.bind(this)).join(', ') +')';
                } else return undefined;
            }
        }

        

        , like: function(command) {
            return ' '+(this._type === 'postgres' ? 'I' : '')+'LIKE '+this._escape(command.value);
        }

        , notLike: function(command) {
            return ' NOT '+(this._type === 'postgres' ? 'I' : '')+'LIKE '+this._escape(command.value);
        }
        


        , 'null': function(){
            return ' is null';
        }


        , 'notNull': function(){
            return ' is not null';
        }
    });
}();
