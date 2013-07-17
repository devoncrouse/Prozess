Prozess
=======
[![Build
Status](https://secure.travis-ci.org/cainus/Prozess.png?branch=master)](http://travis-ci.org/cainus/Prozess)
[![Coverage Status](https://coveralls.io/repos/cainus/Prozess/badge.png?branch=master)](https://coveralls.io/r/cainus/Prozess)

Prozess is a Kafka library for node.js

[Kafka](http://incubator.apache.org/kafka/index.html) is a persistent, efficient, distributed publish/subscribe messaging system.

There are two low-level clients: The Producer and the Consumer:

##Producer example:

```javascript
var Producer = require('prozess').Producer;

var producer = new Producer('social', {host : 'localhost'});
producer.connect();
console.log("producing for ", producer.topic);
producer.on('error', function(err){
  console.log("some general error occurred: ", err);  
});
producer.on('brokerReconnectError', function(err){
  console.log("could not reconnect: ", err);  
  console.log("will retry on next send()");  
});

setInterval(function(){
  var message = { "thisisa" :  "test " + new Date()};
  producer.send(JSON.stringify(message), function(err){
    if (err){
      console.log("send error: ", err);
    } else {
      console.log("message sent");
    }
  });
}, 1000);
```
##Zookeeper consumer example:

A `Zookeeper` object handles broker enumeration and offset storage
```javascript
var Zookeeper = require('prozess').Zookeeper;
var zk = new Zookeeper({
  host: 'kafka00.lan',
  port: 2181
});

var onMessages = function(messages, error, cb) {
  if (error) return console.error(error);
  console.log('Received %d messages', messages.length);

  // true  - (Acknowledge) Update Zk offsets and continue consuming
  // false - (Fail) Resend the same batch in 5 seconds so I don't
  //                have to put it somewhere. TODO: configure wait
  cb(true);
}

// Start consuming
// TODO: Support message filter function argument
zk.consumeTopic('MessageHeaders', 'dcrouse', onMessages);

// Stop consuming
// TODO: Implement

```

##Consumer example:

```javascript
var Consumer = require('prozess').Consumer;

var options = {host : 'localhost', topic : 'social', partition : 0, offset : 0};
var consumer = new Consumer(options);
consumer.connect(function(err){
  if (err) {  throw err; }
  console.log("connected!!");
  setInterval(function(){
    console.log("===================================================================");
    console.log(new Date());
    console.log("consuming: " + consumer.topic);
    consumer.consume(function(err, messages){
      console.log(err, messages);
    });
  }, 7000);
});

```

A `Consumer` can be constructed with the following options (default values as
shown below):

```javascript
var options = {
  topic: 'test',
  partition: 0,
  host: 'localhost',
  port: 9092,
  offset: null, // Number, String or BigNum
  maxMessageSize: Consumer.MAX_MESSAGE_SIZE,
  polling: Consumer.DEFAULT_POLLING_INTERVAL
};
```

##Installation:

     npm install prozess

##Checkout the code and run the tests:

     $ git clone https://github.com/cainus/Prozess.git
     $ cd Prozess ; make test-cov && open coverage.html


##Kafka Compatability matrix:

<table>
  <tr>
     <td>Kakfa 0.8.0 Release</td><td>Not Supported</td>
  </tr>
  <tr>
    <td>Kafka 0.7.2 Release</td><td>Supported</td>
  <tr>
    <td>Kafka 0.7.1 Release</td><td>Supported</td>
  <tr>
    <td>Kafka 0.7.0 Release</td><td>Supported</td>
  <tr>
    <td>kafka-0.6</td><td>Consumer-only support.</td>
  <tr>
    <td>kafka-0.05</td><td>Not Supported</td>
</table>

Versions taken from http://incubator.apache.org/kafka/downloads.html
