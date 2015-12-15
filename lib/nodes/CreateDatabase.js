(function() {
    'use strict';


    let Class = require('ee-class');




    module.exports = new Class({


        // the name of the node
        name: 'createDatabase'



        /**
         * renders the node
         *
         * @param {object} connection database connection
         * @param {object} definition the nodes the definition
         *
         * @returns {promise} a promise to be resolved
         */
        , render: function(parameters, definition) {
            if (type.object(definition.databaseName)) {
                return this.renderObject(definition.databaseName).then((output) => {
                    return Promise.resolve(`CREATE DATABASE ${output}`);
                });
            }
            else return Promise.resolve(`CREATE DATABASE ${parameters.escapeId(definition.databaseName)}`);
        }



        


        /**
         * validates the input given by the definition
         *         
         * @param {object} definition the nodes the definition
         *
         * @returns {promise} a promise to be resolved
         */
        , validate: function(definition) {
            return this.check(definition)
                .property('databaseName').string().or().object()
                .execute();
        }
    });
}();
