(function() {
    'use strict';


    let Class = require('ee-class');




    module.exports = new Class({


        // the name of the node
        name: 'createSchema'



        /**
         * renders the node
         *
         * @param {object} connection database connection
         * @param {object} definition the nodes the definition
         *
         * @returns {promise} a promise to be resolved
         */
        , render: function(parameters, definition) {
            if (type.object(definition.schemaName)) {
                return this.renderObject(definition.schemaName).then((output) => {
                    return Promise.resolve(`CREATE SCHEMA ${output}`);
                });
            }
            else return Promise.resolve(`CREATE SCHEMA ${parameters.escapeId(definition.schemaName)}`);
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
                .property('schemaName').string().or().object()
                .execute();
        }
    });
}();
