const express = require('express');
const kafka = require('kafka-node');
const { Client } = require('@elastic/elasticsearch');
const es_client = new Client({ node: 'http://10.0.100.51:9200' });
const sha256 = require('sha256');
const _ = require('lodash');

const kafkaHost = "10.0.100.23:9092"
const topic = "requests"
const client = new kafka.KafkaClient({ kafkaHost: kafkaHost });
const producer = new kafka.HighLevelProducer(client);

const app = express();

app.use(express.json());
app.listen(5001);

function validate(request) {
    // NEED TO VALIDATE REQUEST!
    let i = 0;
}

async function isPresent(id) {
  let { body } = await es_client.exists({
    index: 'requests',
    id: id
  });

  return body;
}

async function writeToEs(message) {
  let toHash = _.omit(message, 'type');
  id = sha256(JSON.stringify(toHash));
  
  if( !await isPresent(id) ) {
    await es_client.index({
      index: 'requests',
      id: id,
      body: {
        type: message.type,
        text: message.text
      }
    });

    return {
      id: id,
      resp: 'inserted'
    }
  }

    return {
      id: id,
      resp: 'exists'
    };
}

function writeToKafka(id, message, res) {
  message.id = id;
  let payloads = [{ topic: topic, messages: JSON.stringify(message) }];

  producer.send(payloads, (err, data) => {
    console.log(err, data);
    if(err)
      res.status(500).send(JSON.stringify({"messages": "Error while sending messages."}));
    else
      res.status(201).send(JSON.stringify({ "message": message }));
  });
}

producer.on('ready', function () {
    console.log('Producer is ready.');

    app.get('/test', (req, res) => res.send('API IS WORKING!'))

    // Write to Kafka and Elastic Search
    app.post('/predict', async (req, res) => {
        message = req.body;

        console.log(JSON.stringify(message));

        es_resp = await writeToEs(message);
        if (es_resp.resp === 'inserted')
          writeToKafka(es_resp.id, message, res);
        else 
        res.status(201).send(JSON.stringify({ "id": es_resp.id }));
    });

    app.get('/es/:topic', async (req, res) => {
      const result = await es_client.search({
        index: 'requests',
        body: {
          query: {
            match_all: {topic: req.params.topic}
          }
        }
      });

      res.status(201).send(JSON.stringify(result));
    })
});

producer.on('error', err => console.error(err));