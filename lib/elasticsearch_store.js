'use strict';

var Promise = require('bluebird');
var _ = require('lodash');
var elasticsearch_api = require('elasticsearch_api');

module.exports = function(_client, index_name, logger) {
    var client = elasticsearch_api(_client, logger, {index: index_name});

    function parseError(err) {
        if (err.toJSON) {
            var err = err.toJSON();
            if (err.msg) {
                return err.msg;
            }
            else {
                return "Unknown ES Error Format " + JSON.stringify(err);
            }
        }
        if (err.stack) {
            return err.stack
        }
        return err.response ? err.response : err;
    }

    function _createIndex() {
        var existQuery = {index: index_name};

        return client.index_exists(existQuery)
            .then(function(exists) {
                if (!exists) {
                    var mapping = require('./mappings/endpoint.json');

                    // Make sure the index exists before we do anything else.
                    var createQuery = {
                        index: index_name,
                        body: mapping
                    };

                    return client.index_create(createQuery)
                        .then(function(results) {
                            return results;
                        })
                        .catch(function(err) {
                            // It's not really an error if it's just that the index is already there.
                            if (err.match(/index_already_exists_exception/) === null) {
                                var errMsg = parseError(err);
                                logger.error(`Could not create index: ${index_name}, error: ${errMsg}`);
                                return Promise.reject(`Could not create job index, error: ${errMsg}`)
                            }
                        });
                }

                // Index already exists. nothing to do.
                return true;
            })
    }


    function isAvailable() {
        let query = {index: index_name, q: '*'};

        return new Promise(function(resolve, reject) {
            client.search(query)
                .then(function(results) {
                    logger.trace(`index ${index_name} is now available`);
                    resolve(results)
                })
                .catch(function(err) {
                    var isReady = setInterval(function() {
                        client.search(query)
                            .then(function(results) {
                                clearInterval(isReady);
                                resolve(results)
                            })
                            .catch(function(err) {
                                logger.warn('verifying job index is open')
                            })
                    }, 200)
                })
        })
    }

    function init() {
        return new Promise(function(resolve, reject) {
            _createIndex()
                .then(function(results) {
                    return isAvailable();
                })
                .then(function(avaialable) {
                    resolve(true);
                })
                .catch(function(err) {
                    var errMsg = parseError(err);
                    logger.error(errMsg);
                    logger.error(`Error created job index: ${errMsg}`);
                    logger.info(`Attempting to connect to elasticsearch`);

                    var checking = setInterval(function() {
                        return _createIndex()
                            .then(function() {
                                var query = {index: index_name};
                                return client.index_recovery(query)
                            })
                            .then(function(results) {
                                var bool = false;

                                if (Object.keys(results).length !== 0) {
                                    var isPrimary = _.filter(results[index_name].shards, function(shard) {
                                        return shard.primary === true;
                                    });

                                    bool = _.every(isPrimary, function(shard) {
                                        return shard.stage === "DONE"
                                    });
                                }

                                if (bool) {
                                    clearInterval(checking);
                                    logger.info("connection to elasticsearch has been established");
                                    return isAvailable().then(function(avaialble) {
                                        resolve(true);
                                    })
                                }
                            })
                            .catch(function(err) {
                                var errMsg = parseError(err);
                                logger.info(`Attempting to connect to elasticsearch, error: ${errMsg}`);
                            })
                    }, 3000)

                });
        })
    }

    return {
        init: init,
        search: client.search,
        parseError: parseError
    }
};
