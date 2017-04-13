!function(){
    'use strict';


    var   Class         = require('ee-class')
        , log           = require('ee-log')
        , type          = require('ee-types')
        , Parameters    = require('./Parameters')
        , QueryContext  = require('related-query-context');


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




        // known aggregate functions
        , aggregateFunctions: {
              avg   : 'AVG'
            , sum   : 'SUM'
            , min   : 'MIN'
            , max   : 'MAX'
            , count : 'COUNT'
        }




        // know modes
        , knowModes: {
              query: 'select'
            , select: 'select'
            , update: 'update'
            , insert: 'insert'
            , delete: 'delete'
            , create: 'create'
            , drop: 'drop'
        }






        /**
         * class constructor
         *
         * @param <Object> contains the esacpe and escapeid function
         */
        , init: function init(connection, parameters) {

            // make escaping available to this class
            this.escapeId  = connection.escapeId.bind(connection);
            this.escape    = connection.escape.bind(connection);

            // query paramters object
            this.parameters = parameters || new Parameters();
        }








        /**
         * builds sql queries from query object definitions
         *
         * @param <String>  query mode, defines which query should be built
         * @param <Object>  a query object, definition of the query
         * @param <Object>  parameters object, values that must be
         *                  escaped an inserted into the SQL
         */
        , render: function(queryContext, mode, noCleanup) {
            mode = queryContext.query.mode || mode;

            if (!this.knowModes[mode]) {

                // derefernce external methods
                this.cleanup();

                return Promise.reject(new Error(`Unknwon query mode ${queryContext.query.mode || mode}, cannot render query!`));
            }
            else {

                mode = this.knowModes[mode];

                return this[`_build${mode[0].toUpperCase()+mode.slice(1)}Query`](queryContext.query).then((sql) => {


                    // add the result to the context
                    queryContext.values = this.parameters.getValues();
                    queryContext.sql = sql;


                    // derefernce external methods
                    if (!noCleanup) this.cleanup();


                    // return the sql
                    return Promise.resolve(sql);
                }).catch((err) => {

                    // derefernce external methods
                    this.cleanup();

                    return Promise.reject(err);
                });
            }
        }







        /**
         * dereference items so gc has no problems
         */
        , cleanup: function(err) {

            // derefernce external methods, this class instance
            // is now dead!
            this.escapeId = null;
            this.escape = null;

            this.parameters.cleanup();
            this.parameters = null;
        }







        /**
         * build a create query
         *
         * @param <object> query
         */
        , _buildCreateQuery: function(query) {
            if (type.string(query.schema)) {
                return Promise.resolve(`CREATE SCHEMA ${this.escapeId(query.schema)};`);
            }
            else if (type.string(query.database)) {
                return Promise.resolve('CREATE DATABASE '+this.escapeId(query.database)+';');
            }
            else {
                return Promise.reject(new Error('Unknown create statement!'));
            }
        }





        /**
         * build a drop query
         *
         * @param <object> query
         */
        , _buildDropQuery: function(query) {
            if (type.string(query.schema)) {
                return Promise.resolve(`DROP SCHEMA ${this.escapeId(query.schema)};`);
            }
            else if (type.string(query.database)) {
                return Promise.resolve('DROP DATABASE '+this.escapeId(query.database)+';');
            }
            else {
                return Promise.reject(new Error('Unknown drop statement!'));
            }
        }






        /**
         * creates an SQL delete statement
         *
         * @param <Object>  a query object, definition of the query
         */
        , _buildDeleteQuery: function(query) {
            var   SQLString = 'DELETE FROM '
                , keys      = []
                , values    = [];

            // from
            SQLString += this._renderFrom(query.database, query.from);


            // we need to render a subquery for every selected field (primary key)
            SQLString += ' WHERE ' + this._renderUpdateOrDeleteFilter(query);

            return Promise.resolve(SQLString);
        }






        /**
         * creates an SQL insert statement
         *
         * @param <Object>  a query object, definition of the query
         */
        , _buildInsertQuery: function(query) {
            var   SQLString = 'INSERT INTO '
                , keys = []
                , values = [];

            // from
            SQLString += this._renderFrom(query.database, query.from);

            Object.keys(query.values).forEach(function(key) {
                keys.push(this.escapeId(key));
                values.push(this.parameters.set(key, query.values[key], true));
            }.bind(this));


            // values or default values
            if (keys.length) SQLString += ' ('+keys.join(', ')+') VALUES ('+values.join(', ')+')';
            else SQLString += this._insertWithoutValues();

            // return values
            if (query.returning && query.returning.length) SQLString += this._returningColumns(query.returning);


            return Promise.resolve(SQLString);
        }




        /**
         * define which columns return after an insert
         *
         * @param <Array> values to return
         */
        , _returningColumns: function(dictionary) {
            return ' RETURNING '+(dictionary || []).map(function(key) {
                return this.escapeId(key);
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
         */
        , _buildUpdateQuery: function(query, parameters) {
            var   SQLString = 'UPDATE '
                , updates   = [];

            // from
            SQLString += this._renderFrom(query.database, query.from);

            SQLString += ' SET ';
            Object.keys(query.values).forEach(function(key) {
                updates.push(this._renderUpdateValue(key, query.values[key]));
            }.bind(this));

            SQLString += updates.join(', ') +' ';


            // we need to render a subquery for every selected field (primary key)
            SQLString += ' WHERE ' + this._renderUpdateOrDeleteFilter(query);


            return Promise.resolve(SQLString);
        }


        /**
         * render the filter for the update query
         *
         * @param <Object>  query
         */
        , _renderUpdateOrDeleteFilter: function(query) {
            return query.select.map(function(column) {

                let subQuery = {
                      select    : [column]
                    , from      : query.from
                    , database  : query.database
                    , filter    : query.filter
                    , join      : query.join
                    , order     : query.order
                    , group     : query.group
                    , limit     : query.limit
                    , offset    : query.offset
                    , mode      : 'select'
                };

                return this.escapeId(query.from)+'.'+this.escapeId(column)+' IN('+this._buildSelectQuerySync(subQuery)+')'
            }.bind(this)).join(' AND ')
        }



        /**
         * add update specific commands
         *
         * @param <String>  column to modify
         * @param <mixed>   the actual value or instruction
         */
        , _renderUpdateValue: function(filedName, value) {
            var fn;

            if (type.function(value)) {
                fn = value();

                switch(fn.fn) {
                    case 'increaseBy':
                        return this.escapeId(filedName)+' = '+this.escapeId(filedName)+' + '+this.parameters.set(filedName, fn.value, true);

                    case 'decreaseBy':
                        return this.escapeId(filedName)+' = '+this.escapeId(filedName)+' - '+this.parameters.set(filedName, fn.value, true);

                    default:
                        throw new Error('Failed to identify the value for the field «'+filedName+'»! unknown function «'+fn.fn+'» provided!');
                };

            }
            else return this.escapeId(filedName)+' = '+this.parameters.set(filedName, value, true);
        }




        /**
         * creates an SQL select statement
         *
         * @param <Object>  a query object, definition of the query
         */
        , _buildSelectQuery: function(query){
            return Promise.resolve(this._buildSelectQuerySync(query));
        }






        /**
         * what a fucking hack. need a synchronous version of the select query builder
         */
        , _buildSelectQuerySync: function(query) {
            var SQLString = ''
                , fromString = ''
                , filter;

            if (type.object(query.from)) {
                // subqueries
                fromString = ' FROM (' + this._buildSelectQuerySync(query.from.query) + ') AS '+this.escapeId(query.from.alias);
                query.from = query.from.alias;
            }
            else fromString = ' FROM ' + this._renderFrom(query.database, query.from);


            // render select
            SQLString += ' SELECT ' + (this._renderSelect((query.select || []), query.from) || '*');

            // from
            SQLString += fromString;

            // joins
            if (query.join && query.join.length) SQLString += this._renderJoin(query.database, query.join);

            // render filter (where statement)
            if (query.filter && Object.keys(query.filter).length) filter = this._renderFilter(query, query.filter || {});
            if (filter && filter.length && filter.trim().length) SQLString += ' WHERE ' + filter;

            // order & group
            SQLString += this._renderOrderAndGroup(query);

            // limit & offset
            SQLString += this._renderLimit(query);


            return SQLString;
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
                if (type.string(grouping)) return this.escapeId(grouping);
                else return `${grouping.table ? `${this.escapeId(grouping.table)}.` : ''}${this.escapeId(grouping.column)}`;
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


                SQLString += this.escapeId(database) + '.' + this.escapeId(join.target.table) + (join.alias ? ' as '+this.escapeId(join.alias) : '') +' ON ';
                SQLString += this.escapeId(join.source.table) +'.'+this.escapeId(join.source.column)+'=';
                SQLString += this.escapeId(join.alias || join.target.table) +'.'+this.escapeId(join.target.column);
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
            return this.escapeId(database || 'undefined') + '.' + this.escapeId(table || 'undefined');
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
                        instructions.push(this.escapeId(instruction));
                        break;

                    case 'object':
                        if (instruction.byArray) this._renderOrderByValue(instruction, instructions);
                        else if (instruction.entity) {
                            if (instruction.fn) instructions.push(`${instruction.fn}(${this.escapeId(instruction.entity)}.${this.escapeId(instruction.property)}) ${(instruction.desc ? 'DESC' : 'ASC')}`);
                            else instructions.push(this.escapeId(instruction.entity)+'.'+this.escapeId(instruction.property) + (instruction.desc ? ' DESC' : ' ASC'));
                        }
                        else if (instruction.raw) instructions.push(instruction.raw + (instruction.desc ? ' DESC' : ' ASC'));
                        else instructions.push(this.escapeId(instruction.property) + (instruction.desc ? ' DESC' : ' ASC'));
                        break;

                    default:
                        throw new Error('Invalid type «'+type(instruction)+'» for order statement!').setName('InvalidOrderException');
                }
            }.bind(this));


            return instructions.length ? instructions.join(', ') : '1';
        }


        /**
            instructions.push('FIELD('+this.escapeId(instruction.entity)+'.'+this.escapeId(instruction.property)+', '+(instruction.desc ? instruction.byArray.reverse() : instruction.byArray).join(', ')+')');
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
                statement += ' WHEN '+this.escapeId(instruction.entity)+'.'+this.escapeId(instruction.property)+' = '+this.escape(item)+' THEN '+(++index)
            }.bind(this));

            statement += ' ELSE '+(++index)+' END ';

            instructions.push(statement);
        }



        /**
         * creates an SQL select statement
         *
         * @param <Arary>   select instructions
         * @param <String>  table to select on
         */
        , _renderSelect: function(select, tablename) {
            var selects = []
                , command;


            if (!select.length) selects.push(this.escapeId(tablename)+'.*');

            select.forEach(function(selector){
                switch (type(selector)) {
                    case 'string':
                        selects.push(this.escapeId(tablename)+'.'+(selector === '*' ? '*' : this.escapeId(selector)));
                        break;

                    // subquery
                    case 'object':
                        if (selector.isRelatedSelector && selector.isRelatedSelector()) {
                            selects.push(selector.render(select, this));
                            this.parameters.addSelector(selector.aliasEntityName, selector.id, selector);
                        }
                        else selects.push('('+this._buildSelectQuerySync(this._prepareQuery(selector.query))+') as '+this.escapeId(selector.alias || 'undefined'));
                        break;

                    // functions
                    case 'function':
                        command = selector();

                        if (command.table) {
                            selects.push(this.escapeId(command.table)+'.'+this.escapeId(command.column)+(command.alias ? ' as '+ command.alias :''));
                        } else if (command.fn) {
                            selects.push(this.renderSelectFunction(tablename, command));
                        } else if (command.keyWord) {
                            selects.push(command.keyWord);
                        } else if (command.functionName) {
                            selects.push(this.renderSelectUserFunction(tablename, command));
                        }
                        break;
                }
            }.bind(this));

            return selects.join(', ');
        }




        /**
         * creates an SQL where statement including aubqueries
         *
         * @param <mixed>   filter instructions
         * @param <String>  property to apply the current filter on
         * @param <String>  entity to apply the current filter on
         */
        , _renderFilter: function(query, filter, property, entity, dontEscape) {
            var   items = []
                , result
                , id;

            switch (type(filter)) {
                case 'array':
                    filter.forEach(function(filterItem) {
                        result = this._renderFilter(query, filterItem, (property === '_' ? (entity || property) : (property || entity)), entity, dontEscape);
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
                        return (entity ? (this.escapeId(entity) + '.') : '') + (dontEscape ? (property || '') : this.escapeId(property || ''))+' = ('+this._buildSelectQuerySync(this._prepareQuery(filter))+')';
                    }
                    else if (filter.isFulltext) {
                        const fulltextQuery = this.renderFullTextSearch(filter, property, entity, dontEscape);

                        query.group.push({
                              table: entity
                            , column: property
                        });

                        query.order.unshift({
                              raw: ` ts_rank_cd(${fulltextQuery.replace('@@', ', ')})`
                            , desc: true
                        });

                        return fulltextQuery;
                    }
                    else {
                        Object.keys(filter).forEach(function(name) {
                            result = this._renderFilter(query, filter[name], name, property, dontEscape);
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
                    return (entity ? (this.escapeId(entity) + '.') : '') + (dontEscape ? (property || '') : this.escapeId(property || '')) + ' = '+this.parameters.set(property, filter, true);


                case 'null':
                case 'undefined':
                    return (entity ? (this.escapeId(entity) + '.') : '') + (dontEscape ? (property || '') : this.escapeId(property || '')) + ' is null';


                case 'function':
                    return this._renderCommand(property, filter(), (entity ? (this.escapeId(entity ) + '.') : ''), dontEscape);


                default:
                    throw new Error('Cannot process the type «'+type(filter)+'» in the MySQL querybuilder!').setName('InvalidTypeException');
            }
        }






        , renderFullTextSearch: function(filter, property, entity, dontEscape) {
            const rootFilter = filter.getRoot();

            return `${(entity ? (this.escapeId(entity) + '.') : '')}${(dontEscape ? (property || '') : this.escapeId(property || ''))} @@ to_tsquery(${(rootFilter.language ? `${this.escape(rootFilter.language)}, ` : '')}${this.escape(this.renderFullTextComponent(rootFilter.value))})`;
        }




        , renderFullTextComponent(element) {
            switch (element.type) {
                case 'and':
                    return `(${element.children.map(child => this.renderFullTextComponent(child)).join(' & ')})`;
                case  'or':
                    return `(${element.children.map(child => this.renderFullTextComponent(child)).join(' | ')})`;
                case  'value':
                    let value = element.value;

                    if (element.hasWildcardBefore) value = `*:${value}`;
                    if (element.hasWildcardAfter) value = `${value}:*`;
                    if (element.isNot) value = `! ${value}`;
                    return value;
            }
        }





        /**
         * handles filter statements that are functions, this is usually
         * some advanced functionality like subqueries
         *
         * @param <String>  property to apply the current filter on
         * @param <Object>  the command extracted from the function
         * @param <String>  entity to apply the current filter on
         */
        , _renderCommand: function(property, command, entity, dontEscape) {
            var result;

            // comparison
            if (command.operator) {

                // must be a valid operator
                if (!this._operators[command.operator]) throw new Error('Unknown operator «'+command.operator+'»!').setName('InvalidOperatorError');

                // is it a subquery or is it scalar value?
                if (command.query) {
                    return entity+(dontEscape ? property : this.escapeId(property)) + ' ' + this._operators[command.operator] + ' (' + this._buildSelectQuerySync(command.query) +')';
                }
                else if (command && command.value && type.function(command.value.isQuery) && command.value.isQuery()) {
                    return entity+(dontEscape ? property : this.escapeId(property))+' '+this._operators[command.operator]+ ' (' + this._renderSubQuery(command.value) +')';
                }
                else {
                    return entity+(dontEscape ? property : this.escapeId(property)) + ' ' + this._operators[command.operator] + ' '+this.parameters.set(property, command.value, true);
                }
            }

            // function
            else if (command.fn) {
                if (!this[command.fn]) throw new Error('Unknown function «'+command.fn+'»!').setName('InvalidOperatorError');

                // execute the function
                result = this[command.fn](command, property, entity);

                // used for empty in queries
                if (result === false) return 'false';

                // normal filter
                else if (result !== undefined) {
                    if (command.rightSide) return this._renderFilter(command.query, command.value, result, '', true);
                    else return entity+(dontEscape ? property : this.escapeId(property)) + ' ' + result;
                }

                // used for empty not in queries
                else return undefined;
            }

            // unknown
            else throw new Error('Unknwon command «'+JSON.stringify(command)+'»!').setName('InvalidTypeException')
        }



        /**
         * renders a complete query and returns it
         *
         * @param <Object>  a queryBuilder object
         */
        , _renderSubQuery: function(QueryBuilder) {
            return this._buildSelectQuerySync(this._prepareQuery(QueryBuilder));
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





        //////////////// FUNCTIONS.JS CODE ////////////////




        /* select functions
         *
         * @param <Object> options
         */
        , renderSelectFunction: function(tablename, command) {
            return this.aggregateFunctions[command.fn]+'('+this.escapeId(tablename)+'.'+this.escapeId(command.value)+')'+(command.alias ? ' AS '+this.escapeId(command.alias) : '');
        }



        , renderSelectUserFunction: function(tablename, command) {
            const functionName = /[^a-zA-Z0-9_\-]/g.test(command.functionName) ? 'functionNameContainsInvalidCahracters': command.functionName;
            const parameters = [];

            if (type.array(command.args)) {
                command.args.forEach((arg) => {
                    if (type.object(arg) && !type.undefined(arg.value)) parameters.push(this.escape(arg.value+''));
                    else if (type.string(arg)) parameters.push(this._renderSelect([arg], tablename));
                });
            }


            return `${functionName}(${parameters.join(', ')})${(command.alias ? ' AS '+this.escapeId(command.alias) : '')}`;
        }



        /**
         * reference on other tables
         *
         * @param <Object>  instruction
         */
        , reference: function(command) {
            return '= '+this.escapeId(command.entity)+'.'+this.escapeId(command.column);
        }


        /**
         * SQL in statement
         *
         * @param <Object>  instruction
         */
        , in: function(command) {
            var values = command.values;

            if (type.array(command.values) && command.values.length && type.object(command.values[0]) && type.function(command.values[0].isQuery)) values = values[0];

            if (type.function(values.isQuery)) {
                return ' IN ('+this._renderSubQuery(values)+')';
            }
            else {
                if (values.length) {
                    return ' IN (' +values.map(function(value) {
                        return this.escape(value);
                    }.bind(this)).join(', ') +')';
                } else return false;
            }
        }



        /**
         * SQL not in statement
         *
         * @param <Object>  instruction
         */
        , notIn: function(command) {
            var values = command.values;

            if (type.array(command.values) && command.values.length && type.function(command.values[0].isQuery)) values = values[0];

            if (type.function(values.isQuery)) {
                return ' NOT IN ('+this._renderSubQuery(values)+')';
            }
            else {
                if (values.length) {
                    return ' NOT IN (' +values.map(function(value){
                        return this.escape(value);
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
            return ' ILIKE '+this.escape(command.value);
        }



        /**
         * SQL not like statement
         *
         * @param <Object>  instruction
         */
        , notLike: function(command) {
            return ' NOT ILIKE '+this.escape(command.value);
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
