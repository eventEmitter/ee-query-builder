(function() {
    'use strict';

    let type = require('ee-types');
    let Class = require('ee-class');



    // all available types for type checking
    let types = ['string', 'number', 'boolean', 'array', 'object', 'function', 'date', 'regexp', 'error', 'undefined', 'buffer', 'null', 'symbol'];







    let classDefinition = {


        // property name
        propertyName: ''


        /**
         * this module is instantiated once per node
         * so we need to store the nodes name
         *
         * @param {string} propertyName 
         */
        , init: function(parent, propertyName) {
            this.propertyName = propertyName;
            this.parent = parent;

            this.checks = [];
        }







        /**
         * support for or checks ;)
         */
        , or: function() {
            return this;
        }






        /**
         * call parent
         */
        , property: function(propertyName) {
            return this.parent.property(propertyName);
        }






        /**
         * execute all checks
         */
        , execute: function() {
            return this.parent.execute();
        }





        /**
         * valdiate the checks
         */
        , validate: function(input) {
            if (!this.checks.length) return Promise.resolve();
            else if (this.checks.some((typeName) => {
                return type(input[this.propertyName]) === typeName;
            })) return Promise.resolve();
            else return Promise.reject(new Error(`[${this.nodeName}] Expected one of the types ${this.checks.join(', ')} for the property ${this.propertyName}, got type ${type(input[this.propertyName])}!`));
        }
    };










    /**
     * applies a type check method for all
     * known types
     */
    types.forEach((typeName) => {
        classDefinition[typeName] = function() {
            return this.checks.push(typeName);
        }
    });







    // export the class
    module.exports = new Class(classDefinition);
})();
