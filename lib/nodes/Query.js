(function() {
    'use strict';



    module.exports = new Class({



        init: function(config) {
            this.type       = options.type;
            this.id         = Symbol('query');
            this.schema     = config.schema;
            this.alias      = config.alias;
            this.entity     = config.entity;

            this.filter     = config.filter;
            this.join       = config.join;
            this.select     = config.select;
        }





        , render: function(context) {

        }





        , toJSON: function() {

        }
    });
})();