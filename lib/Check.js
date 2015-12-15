(function() {
    'use strict';

    let type = require('ee-types');
    let Class = require('ee-class');
    let ProperyCheck = require('./ProperyCheck')






    module.exports = new Class({


        // node name
        nodeName: ''


        /**
         * this module is instantiated once per node
         * so we need to store the nodes name
         *
         * @param {string} nodeName 
         * @param {object} input the input object
         */
        , init: function(nodeName, input) {
            this.nodeName = nodeName;
            this.input = input;
            this.checks = [];
        }






        /**
         * execute all checks
         */
        , execute: function() {
            return Promise.all(this.checks.map((property) => {
                return property.validate(this.input);
            }));
        }







        /**
         * lets the user add checks for a property 
         */
        , property: function(propertyName) {
            let check = new ProperyCheck(this, propertyName);

            this.checks.push(check;

            return check;
        }
    });
})();
