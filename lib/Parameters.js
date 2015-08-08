!function() {


    var   Class = require('ee-class')
        , log   = require('ee-log');


    module.exports = new Class({

        // counter for gettin unique identifiers
        counter: 0


        /**
         * set up the class, create storage
         */
        , init: function(options) {
            this.storage = {};
            this.counter = 0;
            this.escapeId = options.escapeId;
            this.escape = options.escape;
        }


        /**
         * set a value
         *
         * @param <String> key
         * @param <mixed> value
         *
         * @returns <identifier>
         */
        , set: function(key, value, withQuestionMark) {

            // remove invalid cahracters
            key = key.replace(/[^a-z0-9]/gi, '').slice(0, 10);

            // create the unqiue key
            key = key + (++this.counter);

            // storae value
            this.storage[key] = value;

            // optional return with questionmark
            return (withQuestionMark ? '?' : '')+key;
        }




        /**
         * returns the storage or only one key
         *
         * @returns <mixed>
         */
        , get: function(key) {
            return key ? this.storage[key] : this.storage;
        }
    });
}();
