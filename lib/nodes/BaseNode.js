(function() {
    'use strict';


    let Class = require('ee-class');
    let Check = require('../Check');
    



    module.exports = new Class({


        // the name of the node
        name: 'unknown'





        /**
         * set the node up
         */
        , init: function(renderContext) {
            this.renderContext = renderContext;
        }






        /**
         * renders any object
         *
         * @param {object} definition
         *
         * @returns {promise}
         */
        , renderObject: function(definition) {
            return this.renderContext.render(definition);
        }






        


        /**
         * returns a initialized check instance
         */
        , check: function(input) {
            return new Check(this.name, input);
        }
    });
}();
