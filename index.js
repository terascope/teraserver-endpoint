'use strict';

var es_store = require('./lib/elasticsearch_store');
var endpoint_store = require('./lib/endpoint_store');
var schema = require('./schema');
var app, client, logger, search, es_service, endpoint_service, index_name, isReady;

var api = {
    _config: undefined,

    config_schema: schema,

    config: function(config) {
        this._config = config;
        logger = config.logger;
        app = config.app;
        client = config.elasticsearch;
    },

    static: function() {
    },

    init: function() {
        var appConfig = api._config;
        search = appConfig.search(appConfig, '@timestamp');

        index_name = `${api._config.context.name}__endpoints`;
        es_service = es_store(client, index_name, logger);
        es_service.init()
            .then(function() {
                isReady = true;
            })
    },

    pre: function() {

    },

    routes: function() {

    },

    post: function() {
        if (isReady) {
            endpoint_service = endpoint_store(api._config, es_service, index_name);
        }
        else {
            var checkIndex = setInterval(function() {
                if (isReady) {
                    clearInterval(checkIndex);
                    endpoint_service = endpoint_store(api._config, es_service, index_name);
                }
            }, 3000);
        }
    }
};

module.exports = api;
