var Zookeeper = require('../lib/Zookeeper');
var Consumer = require('../lib/Consumer');

var zk = new Zookeeper({
    host: 'localhost',
    port: 2181
});

var brokers;
var consumers;

var customOffsets = {
  '0-1': '8204235192',
  '1-1': '8206162155',
  '1-0': '8194593369',
  '2-1': '5974071220',
  '0-0': '8202817649',
  '2-0': '5937425705'
}

zk.getBrokers(function(brokers, error) {
  if (error) {
    console.error('getBrokers error', error);
  }

  console.log('Brokers\n', brokers, '\n');
  this.brokers = brokers;
});

zk.setConsumerOffsets('TestTopic', 'TestConsumerGroup', customOffsets, function(error) {
  if (error) {
    console.error('Error setting consumer offsets', error);
    return;
  }

  zk.getConsumerOffsets('TestTopic', 'TestConsumerGroup', function(offsets, error) {
    if (error) {
      console.error('Error retrieving consumer offsets', error);
    }

    console.log('Consumer offsets\n', offsets, '\n');
  });
});

