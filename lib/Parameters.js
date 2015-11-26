!function() {


    var   Class = require('ee-class')
        , log   = require('ee-log');


    module.exports = new Class({


        /**
         * set up the class, create storage
         */
        init: function(options) {
            this.values = [];

            // we need a storage for building proper configured having statements
            // for aggregate functions
            this.selectedAggregates = {};
        }




        /**
         * store a selector so it can be used by the the filters too
         *
         * @param {string} entitiyName the entity name this selector applies to
         * @param {string} aliasName the alias the selector is working with
         * @apram {Object} selector the selector itself
         */
        , addSelector: function(entitiyName, aliasName, selector) {
            if (!this.selectedAggregates[entitiyName]) this.selectedAggregates[entitiyName] = {};

            this.selectedAggregates[entitiyName][aliasName] = selector;
        }



        /**
         * checks if there is a selector for a specific alias in the filter
         *
         * @param {string} entitiyName the entity name this selector applies to
         * @param {string} aliasName the alias the selector is working with
         *
         * @returns {boolean} true if there is a selector for the given alias and entity
         */
        , hasSeletor: function(entitiyName, aliasName) {
            return this.selectedAggregates[entitiyName] && this.selectedAggregates[entitiyName][aliasName];
        }




        /**
         * returns a selector for a given entity and alias name
         *
         * @param {string} entitiyName the entity name this selector applies to
         * @param {string} aliasName the alias the selector is working with
         *
         * @returns {object} selector
         */
        , getSelector: function(entitiyName, aliasName) {
            if (!this.hasSeletor(entitiyName, aliasName)) throw new Error('Cannot return a selector for the alias «'+entitiyName+'.'+aliasName+'». It does not exist!');
            return this.selectedAggregates[entitiyName][aliasName];
        }




        /**
         * set a value
         *
         * @param <String> key
         * @param <mixed> value
         *
         * @returns <identifier>
         */
        , set: function(key, value) {

            this.values.push(value);

            return this.renderParameter();
        }




        /**
         * returns the values to used for the parameterized query
         */
        , getValues: function() {
            return this.values;
        }



        /**
         *
         */
        , cleanup: function() {
            this.renderParameter = null;
        }
    });
}();
