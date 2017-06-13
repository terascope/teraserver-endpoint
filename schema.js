'use strict';

module.exports = {
    interval: {
        doc: 'time in milliseconds, used to determine when to check updates for changes',
        default: 600000,
        format: function(val) {
            if (isNaN(val)) {
                throw new Error('interval parameter for teraserver-endpoint must be a number')
            }
            else {
                if (val < 0) {
                    throw new Error('interval parameter for teraserver must be a positive number')
                }
            }
        }
    }
};
