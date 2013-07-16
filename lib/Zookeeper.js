var _ = require('underscore');
var bignum = require('bignum');
var zkcli = require('zookeeper');

var Zookeeper = function(options) {
  options = options || {};
  this.zkOpts = {
    connect: (options.host || 'localhost') + ':' + (options.port || 2181),
    timeout: options.timeout || 30000,
    debug_level: Zookeeper.ZOO_LOG_LEVEL_WARN,
    host_order_deterministic: false,
    data_as_buffer: false
  }

  this.offsetsResponder = function(){};
  this.fetchResponder = function(){};
};

Zookeeper.prototype.getBrokers = function(cb) {
  var brokerIdPath = '/brokers/ids';

  var onZkConnect = function(error) {
    if (error) {
      return cb(null, 'Zk connect failed: ' + error);
    }

    zk.a_get_children(brokerIdPath, false, onGetBrokers);
  }

  var onGetBrokers = function(rc, error, children) {
    if (rc != 0) {
      return cb(null, 'Get brokers failed: ' + error);
    }

    var result = [];
    var onBrokersProcessed = _.after(children.length, function(errors) {
      zk.close();
      return cb(result, (errors.length > 0) ? errors : null);
    });

    (children.length == 0) ? onBrokersProcessed() : null;

    var errors = [];
    _.each(children, function(broker) {
      var brokerPath = brokerIdPath + '/' + broker;
      zk.a_get(brokerPath, false, function(rc, error, stat, data) {
        if (rc == 0) {
          var brokerData = _.object(['name', 'host', 'port'], data.split(':'));
          result.push(brokerData)
        } else {
          errors.push(error);
        }

        onBrokersProcessed(errors);
      });
    });
  }

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

Zookeeper.prototype.getConsumerOffsets = function(topic, group, cb) {
  var groupTopicPath = '/consumers/' + group + '/offsets/' + topic;

  var onZkConnect = function(error) {
    if (error) {
      return cb(null, 'Zk connect failed: ' + error);
    }

    zk.a_get_children(groupTopicPath, false, onGetBrokerPartitions);
  }

  var onGetBrokerPartitions = function(rc, error, children) {
    if (rc != 0) {
      return cb(null, 'Get broker partitions failed: ' + error);
    }

    var result = [];
    var errors = [];

    var onOffsetsProcessed = _.after(children.length, function(errors) {
      zk.close();
      return cb(result, (errors.length > 0) ? errors : null);
    });

    (children.length == 0) ? onOffsetsProcessed() : null;

    _.each(children, function(brokerPartition) {
      var brokerPartitionPath = groupTopicPath + '/' + brokerPartition;
      zk.a_get(brokerPartitionPath, false, function(rc, error, stat, data) {
        if (rc == 0) {
          var brokerPartitionData = _.object(['broker', 'partition'], brokerPartition.split('-'));
          result.push({
              broker: brokerPartitionData['broker'],
              partition: brokerPartitionData['partition'],
              offset: data
          });
        } else {
          errors.push(error);
        }
        onOffsetsProcessed(errors);
      });
    });
  }

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

Zookeeper.prototype.setConsumerOffsets = function(topic, group, offsets, cb) {
  if (!offsets || _.keys(offsets).length == 0) {
    return cb('Empty offsets object');
  }

  var groupTopicPath = '/consumers/' + group + '/offsets/' + topic;

  var onZkConnect = function(error) {
    if (error) {
      return cb(error);
    }

    zk.mkdirp(groupTopicPath, onGroupTopicPathCreated);
  }

  var onGroupTopicPathCreated = function(error) {
    if (error) {
      return cb(error);
    }

    _.each(offsets, processOffset);
  }

  var processOffset = function(offset, brokerPartition) {
    var offsetPath = groupTopicPath + '/' + brokerPartition;
    zk.a_exists(offsetPath, false, function(rc, error, stat) {
      if (rc != 0) {
        if (error == 'no node') {
          zk.a_create(offsetPath, offset, null, onOffsetProcessed);
        } else {
          console.error('Zk query failed: Error=%s, Path=%s', error, offsetPath);
        }
      } else {
        zk.a_set(offsetPath, offset, stat.version, onOffsetProcessed);
      }
    });
  }

  var onOffsetProcessed = function(rc, error) {
    if (rc != 0) {
      console.error('Error processing offset', error);
    }

    onOffsetsProcessed();
  }

  var onOffsetsProcessed = _.after(_.keys(offsets).length, function() {
    zk.close();
    return cb();
  });

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

Zookeeper.prototype.onOffsets = function(handler) { this.offsetsResponder = handler; };
Zookeeper.prototype.onFetch = function(handler) { this.fetchResponder = handler; };

module.exports = Zookeeper;

