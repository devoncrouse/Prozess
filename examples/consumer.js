var Consumer = require('../lib/Consumer');

var consumer = new Consumer({
    host: 'localhost',
    topic: 'MessageHeaders',
    partition: 0,
    offset: 0
});

consumer.connect(function(error) {
  if (error) {
    console.error('Error connecting to Kafka', error);
    return;
  }

  console.log('Connected to broker');
  setInterval(function() {
    console.log(new Date(), 'Consuming from ' + consumer.topic + '...');
    consumer.consume(function(consumeError, messages) {
      if (consumeError) {
        return console.log('Error consuming from Kafka', consumeError);
      }

      console.log('Consumer result', messages);
    });
  }, 7000);
});

