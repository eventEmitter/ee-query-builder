!function(){
    'use strict';


    var   Class         = require('ee-class')
        , log           = require('ee-log')
        , type          = require('ee-types')
        , Parameters    = require('./Parameters')
        , Functions     = require('./Functions');


    /**
     * takes a query object and returns a sql string
     */


    module.exports = new Class({

        // some avialbel operators in filters
        _operators: {
              '=':  '='
            , '<':  '<'
            , '>':  '>'
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
         * @param <Object> contains the esacpe and escapeid function
         */
        , init: function init(options, FunctionsConstructor) {
            this._escapeId  = options.escapeId;
            this._escape    = options.escape;

            // get an instance of the filter renderer foir advanced functions
            this._functions = new (FunctionsConstructor || Functions)({
                  escapeId      : options.escapeId
                , escape        : options.escape
                , queryBuilder  : this
            });
        }



        /**
         * the _render() method creates an sql query from an object
         *
         * @param <String>  query mode, defines which query should be built
         * @param <Object>  a query object, definition of the query
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         */
        , _render: function(mode, query, parameters) {

            // maybe we get already some parmeters from a parent query ..
            parameters = parameters || new Parameters({
                  escapeId      : this._escapeId
                , escape        : this._escape
            });

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

                case 'create':
                    return this._buildCreateQuery(query, parameters);

                case 'drop':
                    return this._buildDropQuery(query, parameters);


                default:
                    throw new Error('Unknown query mode «'+mode+'»!').setName('InvalidModeException');
            }
        }



        /**
         * build a create query
         *
         * @param <object> query
         * @param <object> parameters
         */
        , _buildCreateQuery: function(query, parameters) {
            if (type.string(query.database)) {
                return {SQLString: 'CREATE DATABASE '+this._escapeId(query.database)+';', parameters: parameters};
            }
            else if (type.string(query.schema)) {
                return {SQLString: 'CREATE SCHEMA '+this._escapeId(query.schema)+';', parameters: parameters};
            }
            else {
                throw new Error('Unknown create statement!');
            }
        }




        /**
         * build a drop query
         *
         * @param <object> query
         * @param <object> parameters
         */
        , _buildDropQuery: function(query, parameters) {
            if (type.string(query.database)) {
                return {SQLString: 'DROP DATABASE '+this._escapeId(query.database)+';', parameters: parameters};
            }
            else if (type.string(query.schema)) {
                return {SQLString: 'DROP SCHEMA '+this._escapeId(query.schema)+';', parameters: parameters};
            }
            else {
                throw new Error('Unknown drop statement!');
            }
        }
        



        /**
         * creates an SQL delete statement
         *
         * @param <Object>  a query object, definition of the query
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         */
        , _buildDeleteQuery: function(query, parameters) {
            var   SQLString = 'DELETE FROM '
                , keys      = []
                , values    = [];

            // from
            SQLString += this._renderFrom(query.database, query.from);


            // we need to render a subquery for every selected field (primary key)
            SQLString += ' WHERE ' + this._renderUpdateOrDeleteFilter(query, parameters);
            
            return {SQLString: SQLString, parameters: parameters};
        }


        /**
         * creates an SQL insert statement
         *
         * @param <Object>  a query object, definition of the query
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         */
        , _buildInsertQuery: function(query, parameters) {
            var   SQLString = 'INSERT INTO '
                , keys = []
                , values = [];

            // from
            SQLString += this._renderFrom(query.database, query.from);

            Object.keys(query.values).forEach(function(key) {
                keys.push(this._escapeId(key));
                values.push(parameters.set(key, query.values[key], true));
            }.bind(this));


            // values or default values
            if (keys.length) SQLString += ' ('+keys.join(', ')+') VALUES ('+values.join(', ')+')';
            else SQLString += this._insertWithoutValues();

            // return values
            if (query.returning && query.returning.length) SQLString += this._returningColumns(query.returning);
            

            return {SQLString: SQLString, parameters: parameters};
        }




        /**
         * define which columns return after an insert
         *
         * @param <Array> values to return
         */
        , _returningColumns: function(dictionary) {
            return ' RETURNING '+(dictionary || []).map(function(key) {
                return this._escapeId(key);
            }.bind(this)).join(', ');
        }



        /**
         * build an insert query without values to 
         * insert (table defaults)
         */
        , _insertWithoutValues: function() {
            return ' DEFAULT VALUES';
        }


        
        /**
         * creates an SQL update statement
         *
         * @param <Object>  a query object, definition of the query
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         */
        , _buildUpdateQuery: function(query, parameters) {
            var   SQLString = 'UPDATE '
                , updates   = [];

            // from
            SQLString += this._renderFrom(query.database, query.from);

            SQLString += ' SET ';
            Object.keys(query.values).forEach(function(key) {
                updates.push(this._renderUpdateValue(parameters, key, query.values[key]));
            }.bind(this));

            SQLString += updates.join(', ') +' ';


            // we need to render a subquery for every selected field (primary key)
            SQLString += ' WHERE ' + this._renderUpdateOrDeleteFilter(query, parameters);


            return {SQLString: SQLString, parameters: parameters};
        }


        /**
         * render the filter for the update query
         *
         * @param <Object>  query
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         */
        , _renderUpdateOrDeleteFilter: function(query, parameters) {
            return query.select.map(function(column) {
                return this._escapeId(query.from)+'.'+this._escapeId(column)+' IN('+this._render('query', {
                      select    : [column]
                    , from      : query.from
                    , database  : query.database
                    , filter    : query.filter
                    , join      : query.join
                    , order     : query.order
                    , group     : query.group
                    , limit     : query.limit
                    , offset    : query.offset
                }, parameters).SQLString+')'
            }.bind(this)).join(' AND ')
        }



        /**
         * add update specific commands
         *
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         * @param <String>  column to modify
         * @param <mixed>   the actual value or instruction
         */
        , _renderUpdateValue: function(parameters, filedName, value) {
            var fn;

            if (type.function(value)) {
                fn = value();

                switch(fn.fn) {
                    case 'increaseBy':
                        return this._escapeId(filedName)+' = '+this._escapeId(filedName)+' + '+parameters.set(filedName, fn.value, true);

                    case 'decreaseBy':
                        return this._escapeId(filedName)+' = '+this._escapeId(filedName)+' - '+parameters.set(filedName, fn.value, true);

                    default:
                        throw new Error('Failed to identify the value for the field «'+filedName+'»! unknown function «'+fn.fn+'» provided!');
                };

            }
            else return this._escapeId(filedName)+' = '+parameters.set(filedName, value, true);
        }




        /**
         * creates an SQL select statement
         *
         * @param <Object>  a query object, definition of the query
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         */
        , _buildSelectQuery: function(query, parameters){
            var SQLString = ''
                , fromString = ''
                , filter;

            if (type.object(query.from)) {
                // subqueries
                fromString = ' FROM (' + this._buildSelectQuery(query.from.query, parameters).SQLString + ') AS '+this._escapeId(query.from.alias);
                query.from = query.from.alias;
            }
            else fromString = ' FROM ' + this._renderFrom(query.database, query.from);


            // render select            
            SQLString += ' SELECT ' + (this._renderSelect(parameters, (query.select || []), query.from) || '*');

            // from
            SQLString += fromString;

            // joins
            if (query.join && query.join.length) SQLString += this._renderJoin(query.database, query.join);

            // render filter (where statement)
            if (query.filter && Object.keys(query.filter).length) filter = this._renderFilter(parameters, query.filter || {});
            if (filter && filter.length && filter.trim().length) SQLString += ' WHERE ' + filter;

            // order & group
            SQLString += this._renderOrderAndGroup(query);

            // limit & offset
            SQLString += this._renderLimit(query);


            return {SQLString: SQLString, parameters: parameters};
        }



        /**
         * render the ôrder ang group statements
         *
         * @param <Object>  query
         */
        , _renderOrderAndGroup: function(query) {
            var str = '';

            // render group statement
            if(query.group && query.group.length) str += ' GROUP BY ' + (this._renderGroup(query.group ) || 1);

            // render order statement
            if(query.order && query.order.length) str += ' ORDER BY ' + (this._renderOrder(query.order || []) || 1);

            return str;
        }



        /**
         * render thje limit / offset statements
         *
         * @param <Object>  query
         */
        , _renderLimit: function(query) {
            var str = '';

            if(query.limit && !isNaN(parseInt(query.limit, 10))) str += ' LIMIT ' + parseInt(query.limit, 10);
            if(query.offset && !isNaN(parseInt(query.offset, 10))) str += ' OFFSET ' + parseInt(query.offset, 10);

            return str;
        }



        /**
         * creates an SQL group statement
         *
         * @param <Array>   array of fields to use in the group statement
         */
        , _renderGroup: function(group) {
            return group.map(function(grouping) {
                if (type.string(grouping)) return this._escapeId(grouping);
                else return this._escapeId(grouping.table || '') + '.' + this._escapeId(grouping.column || '');
            }.bind(this)).join(', ');
        }




        /**
         * creates an SQL join statement
         *
         * @param <String>  the database to add the joins on
         * @param <Array>   the join instructions
         */
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




        /**
         * creates an SQL from statement
         *
         * @param <String>  the database 
         * @param <String>  the table
         */
        , _renderFrom: function(database, table) {
            return this._escapeId(database || 'undefined') + '.' + this._escapeId(table || 'undefined');
        }




        /**
         * creates an SQL order statement
         *
         * @param <Array>   the order instructions 
         */
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
                        if (instruction.byArray) this._renderOrderByValue(instruction, instructions);
                        if (instruction.entity) instructions.push(this._escapeId(instruction.entity)+'.'+this._escapeId(instruction.property) + (instruction.desc ? ' DESC' : ' ASC'));
                        else instructions.push(this._escapeId(instruction.property) + (instruction.desc ? ' DESC' : ' ASC'));
                        break;

                    default:
                        throw new Error('Invalid type «'+type(instruction)+'» for order statement!').setName('InvalidOrderException');
                }
            }.bind(this));


            return instructions.length ? instructions.join(', ') : '1';
        }


        /**
            instructions.push('FIELD('+this._escapeId(instruction.entity)+'.'+this._escapeId(instruction.property)+', '+(instruction.desc ? instruction.byArray.reverse() : instruction.byArray).join(', ')+')');
        */

        /**
         * order by specific values
         *
         * @param <Object> the instruction
         * @param <Array> array of rendered instructions
         */
        , _renderOrderByValue: function(instruction, instructions) {
            var   statement = 'CASE'
                , index = 0;

            (instruction.desc ? instruction.byArray.reverse() : instruction.byArray).forEach(function(item) {
                statement += ' WHEN '+this._escapeId(instruction.entity)+'.'+this._escapeId(instruction.property)+' = '+this._escape(item)+' THEN '+(++index)
            }.bind(this));

            statement += ' ELSE '+(++index)+' END ';

            instructions.push(statement);
        }



        /**
         * creates an SQL select statement
         *
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         * @param <Arary>   select instructions
         * @param <String>  table to select on
         */
        , _renderSelect: function(parameters, select, tablename) {
            var selects = []
                , command;


            if (!select.length) selects.push(this._escapeId(tablename)+'.*'); 

            select.forEach(function(selector){
                switch (type(selector)) {
                    case 'string':
                        selects.push(this._escapeId(tablename)+'.'+(selector === '*' ? '*' : this._escapeId(selector)));
                        break;

                    // subquery
                    case 'object':
                        if (selector.isRelatedSelector && selector.isRelatedSelector()) {
                            selects.push(selector.render(select, parameters));
                        }
                        else selects.push('('+this._render('query', this._prepareQuery(selector.query), parameters).SQLString+') as '+this._escapeId(selector.alias || 'undefined'));
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
         * creates an SQL where statement including aubqueries
         *
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         * @param <mixed>   filter instructions
         * @param <String>  property to apply the current filter on
         * @param <String>  entity to apply the current filter on
         */
        , _renderFilter: function(parameters, filter, property, entity) {
            var   items = []
                , result
                , id;

            switch (type(filter)) {
                case 'array':
                    filter.forEach(function(filterItem) {
                        result = this._renderFilter(parameters, filterItem, (property === '_' ? (entity || property) : (property || entity)), entity);
                        if (result !== undefined) items.push(result);
                    }.bind(this));

                    if (items.length) {
                        if (items.length === 1) return items[0];
                        else return '(' + items.join(filter.mode === 'and' ? ' AND ' : ' OR ') + ')';
                    }
                    else return undefined


                case 'object':
                    if (type.function(filter.isQuery) && filter.isQuery()) {

                        // returns it
                        return this._escapeId(entity)+'.'+this._escapeId(property)+' = ('+this._render('query', this._prepareQuery(filter), parameters).SQLString+')';
                    }
                    else {
                        Object.keys(filter).forEach(function(name) {
                            result = this._renderFilter(parameters, filter[name], name, property);
                            if (result !== undefined) items.push(result);
                        }.bind(this));

                        if (items.length) {
                            if (items.length === 1) return items[0];
                            else return '(' + items.join(' AND ') + ')';
                        }
                        else return undefined
                    }
                    break;

                case 'string':
                case 'number':
                case 'date':
                case 'boolean':
                    return (entity ? (this._escapeId(entity ) + '.') : '') + this._escapeId(property || '') + ' = '+parameters.set(property, filter, true);


                case 'null':
                case 'undefined':
                    return (entity ? (this._escapeId(entity ) + '.') : '') + this._escapeId(property || '') + ' is null';
                

                case 'function':
                    return this._renderCommand(property, filter(), parameters, (entity ? (this._escapeId(entity ) + '.') : ''));


                default: 
                    throw new Error('Cannot process the type «'+type(filter)+'» in the MySQL querybuilder!').setName('InvalidTypeException');
            }
        }





        /**
         * handles filter statements that are functions, this is usually
         * some advanced functionality like subqueries
         *
         * @param <String>  property to apply the current filter on
         * @param <Object>  the command extracted from the function
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         * @param <String>  entity to apply the current filter on
         */
        , _renderCommand: function(property, command, parameters, entity) {
            var result;

            // comparison
            if (command.operator) {

                // must be a valid operator
                if (!this._operators[command.operator]) throw new Error('Unknown operator «'+command.operator+'»!').setName('InvalidOperatorError');

                // is it a subquery or is it scalar value?
                if (command.query) {
                    return entity+this._escapeId(property) + ' ' + this._operators[command.operator] + ' (' + this._render('query', command.query, parameters).SQLString +')';
                }
                else if (command && command.value && type.function(command.value.isQuery) && command.value.isQuery()) {                 
                    return entity+this._escapeId(property)+' '+this._operators[command.operator]+ ' (' + this._renderSubQuery(command.value, parameters) +')';
                }
                else {
                    return entity+this._escapeId(property) + ' ' + this._operators[command.operator] + ' '+parameters.set(property, command.value, true);
                }
            }

            // function
            else if (command.fn) {
                if (!this._functions[command.fn]) throw new Error('Unknown function «'+command.fn+'»!').setName('InvalidOperatorError');
                result = this._functions[command.fn](command, parameters);
                if (result !== undefined) return entity+this._escapeId(property) + ' ' + result;
                return undefined;               
            }

            // unknown
            else throw new Error('Unknwon command «'+JSON.stringify(command)+'»!').setName('InvalidTypeException')
        }



        /**
         * renders a complete query and returns it
         *
         * @param <Object>  a queryBuilder object
         * @param <Object>  parameters object, values that must be 
         *                  escaped an inserted into the SQL
         */
        , _renderSubQuery: function(QueryBuilder, parameters) {
            return this._render('query', this._prepareQuery(QueryBuilder), parameters).SQLString;
        }




        /**
         * prepare s ausbquery for execution
         *
         * @param <Object> queryBuilder instance
         */
        , _prepareQuery: function(queryBuilder) {
            var resource = queryBuilder.getrootResource();

            resource.setSelectMode();
            resource.prepare(null, true);

            return resource.query;
        }





        /**
         * converts types to db interpretable representations
         *
         * @param <Mixed> input
         */
        , _toString: function(input) {
            switch(type(input)) {
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
                    return input.getFullYear()+'-'+
                           this.input(input.getMonth()+1, 2)+'-'+
                           this.input(input.getDay(), 2)+' '+
                           this.input(input.getHours(), 2)+':'+
                           this.input(input.getMinutes(), 2)+':'+
                           this.input(input.getSeconds(), 2);
                case 'function':
                    //return input();

                default:
                    throw new Error('Failed to convert type «'+type(input)+'» to string!').setName('InvalidTypeException')
            }
        }



        /**
         * pag to the right with zeroes
         */
        , _pad: function(input, length) {
            input = input + '';

            if (input.length < length) {
                return (new Array(length - input.length+1).join('0'))+input;
            }
            else return input;
        }
    });
}();
