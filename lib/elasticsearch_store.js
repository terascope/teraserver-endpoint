'use strict';

var Promise = require('bluebird');
var _ = require('lodash');

module.exports = function(client, index_name, logger) {
    //TODO get rid of this config obj and in search
    var config = {};

    function index_exists(query) {

        return new Promise(function(resolve, reject) {
            var errHandler = _errorHandler(exists, query, reject, logger);

            function exists() {
                client.indices.exists(query)
                    .then(function(results) {
                        resolve(results);
                    })
                    .catch(errHandler);
            }

            exists();
        })
    }

    function index_recovery(query) {

        return new Promise(function(resolve, reject) {
            var errHandler = _errorHandler(indexRecovery, query, reject, logger);

            function indexRecovery() {
                client.indices.recovery(query)
                    .then(function(results) {
                        resolve(results);
                    })
                    .catch(errHandler);
            }

            indexRecovery();
        })
    }

    function index_create(query) {

        return new Promise(function(resolve, reject) {
            var errHandler = _errorHandler(indexCreate, query, reject, logger);

            function indexCreate() {
                client.indices.create(query)
                    .then(function(results) {
                        resolve(results);
                    })
                    .catch(errHandler);
            }

            indexCreate();
        })
    }

    function retry(retryTimer, fn, data) {
        let timer = Math.floor(Math.random() * (retryTimer.limit - retryTimer.start) + retryTimer.start);

        if (retryTimer.limit < 60000) {
            retryTimer.limit += 10000
        }
        if (retryTimer.start < 30000) {
            retryTimer.start += 5000
        }
        setTimeout(function() {
            fn(data);
        }, timer);
    }

    function _errorHandler(fn, data, reject, logger) {
        let retryTimer = {start: 5000, limit: 10000};

        return function(err) {
            if (_.get(err, 'body.error.type') === 'es_rejected_execution_exception') {
                retry(retryTimer, fn, data)
            }
            else {
                var errMsg = parseError(err);
                logger.error(errMsg);
                reject(errMsg)
            }
        }
    }

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

        return index_exists(existQuery)
            .then(function(exists) {
                if (!exists) {
                    var mapping = require('./mappings/endpoint.json');

                    // Make sure the index exists before we do anything else.
                    var createQuery = {
                        index: index_name,
                        body: mapping
                    };

                    return index_create(createQuery)
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

    function search(query) {
        let retryTimer = {start: 5000, limit: 10000};
        let isCounting = query.size === 0;

        return new Promise(function(resolve, reject) {
            function searchES() {
                client.search(query)
                    .then(function(data) {
                        if (data._shards.failed > 0) {
                            var reasons = _.uniq(_.flatMap(data._shards.failures, function(shard) {
                                return shard.reason.type
                            }));

                            if (reasons.length > 1 || reasons[0] !== 'es_rejected_execution_exception') {
                                var errorReason = reasons.join(' | ');
                                logger.error('Not all shards returned successful, shard errors: ', errorReason);
                                reject(errorReason)
                            }
                            else {
                                retry(retryTimer, searchES)
                            }
                        }
                        else {
                            if (isCounting) {
                                resolve(data.hits.total)
                            }

                            if (config.full_response) {
                                resolve(data)
                            }
                            else {
                                resolve(_.map(data.hits.hits, function(hit) {
                                    return hit._source
                                }));
                            }
                        }
                    })
                    .catch(function(err) {
                        if (_.get(err, 'body.error.type') === 'reduce_search_phase_exception') {
                            var retriableError = _.every(err.body.error.root_cause, function(shard) {
                                return shard.type === 'es_rejected_execution_exception';
                            });
                            //scaffolding for retries, just reject for now
                            if (retriableError) {
                                var errMsg = parseError(err);
                                logger.error(errMsg);
                                reject(errMsg)
                            }
                        }
                        else {
                            var errMsg = parseError(err);
                            logger.error(errMsg);
                            reject(errMsg)
                        }
                    });
            }

            searchES();
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
                                return index_recovery(query)
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
        search: search,
        parseError: parseError
    }
};
