'use strict';

var _ = require('lodash');

module.exports = function(config, es_service, index_name) {
    var endpoints = {};
    var isChecking = false;
    var logger = config.logger;
    var app = config.app;
    var client = config.elasticsearch;
    var search = config.search(config, '@timestamp');
    var interval = config.context.sysconfig['teraserver-endpoint'].interval;

    function getEndpoints() {
        return es_service.search({index: index_name, q: '*'})
    }

    function findRoute(route) {
        var index = null;

        _.each(app._router.stack, function(routeConfig, ind) {
            if (routeConfig.name === 'endpoint' && route.match(routeConfig.regex) !== null) {
                index = ind
            }
        });

        return index
    }

    function removeRoute(path) {
        var found = findRoute(path);

        if (found) {
            app._router.stack.splice(found, 1)
        }
    }

    function loadEndpoints() {
        if (!isChecking) {
            isChecking = true;
            return getEndpoints()
                .then(function(_currentEndpoints) {
                    var currentEndpoints = _currentEndpoints.reduce(function(prev, curr) {
                        prev[curr.endpoint] = curr;
                        return prev;
                    }, {});

                    _.forOwn(endpoints, function(config, key) {
                        //if it doesn't exists on currentEndpoints, it needs to be deleted 
                        if (!currentEndpoints[key]) {
                            logger.warn(`removing endpoint ${key}`);
                            removeRoute(config.endpoint)
                        }
                        else {
                            //check if config is the same, if not then change the handle function for the route
                            if (!_.isEqual(config, currentEndpoints[key])) {
                                logger.warn(`Configuration for endpoint ${key} has changed, changing its handle`)
                                let index = findRoute(key);

                                app._router.stack[index].handle = function endpoint(req, res) {
                                    var queryConfig = _.assign({}, currentEndpoints[key]);
                                    queryConfig.es_client = client;

                                    search.luceneQuery(req, res, config.index, queryConfig);
                                }
                            }
                        }
                    });

                    _.forOwn(currentEndpoints, function(config, key) {
                        //if it is not already on endpoints, then we need to create a route
                        if (!endpoints[key]) {
                            logger.info(`Setting endpoint ${key} with configuration: ${JSON.stringify(config)}`);

                            app.use(config.endpoint, function endpoint(req, res) {
                                var queryConfig = _.assign({}, config);
                                queryConfig.es_client = client;

                                search.luceneQuery(req, res, config.index, queryConfig);
                            });
                        }
                    });

                    endpoints = currentEndpoints;
                    isChecking = false;
                })
        }
        else {
            return Promise.resolve(true)
        }
    }

    loadEndpoints()
        .then(function() {
            setInterval(function() {
                loadEndpoints()
            }, interval)
        })
        .catch(function(err) {
            var errMsg = es_service.parseError(err);
            logger.error(`Error while loading endpoints, error: ${errMsg}`)
        })

};
