# SAPERE
The following project describes the implementation process of scalable big data stream processing framework that covers the entire flow of data: from ingestion, enrichment, storage and visualization.

The aim is to create a platform that can be used not only as a valuable enricher, but also that lets its users to understand what the best enricher for their specific use case is: in a time where machine learning science is exponentially growing, so does the effort that one needs to compare all the different algorithms.

The extensibility of the architecture allows a more structured approach for this task by giving the user not only to choose what tool to use but even to easily connect their own solution to it: the platform is designed to add new algorithms in "plug and play" approach using arbitrary languages.

Algorithms will consume the raw data present in our database and apply their prediction, making the data more valuable and understandable.

The data will be received through an **API**, which will assign a unique ID to the request itself, sent to **Elastic Search** to efficiently store and cache it.
Then the unique ID OR request will be sent to a general **Kafka** topic in order to achieve a **scalable**, **fast** messaging system.

A dispatcher, built in **PySpark** and/or in **Java**, connects to the Kafka topic where API data is being sent, and forwards those request to another Kafka topic in order to provide data to consumers.

A cluster of spark machines will subscribe to a specif kafka topic, from which they are going to read data, make predictions and send data back to the dispatcher, so that it can be indexed on elastic search.
![architecture image](/docs/architecture.svg)

# Start a local instance

In order to bring everything up we need to follow some simple steps:

## Create the docker sub network
Since every piece of our architecture needs to communicate with each other we need to create a *subnet* that allows us to do so.
- **Create the docker subnet**:
  Inside the `/bin` folder type ```./network.sh```

## Kafka and Elastic Search
1. **Start kafka**:
   Inside `/kafka` folder type ```docker-compose up```  in your terminal and hit enter.
   At the moment this creates three different topics:
    - *requests*: this is a general topic which contains data sent from the API.
    - *processRequest*: preprocessed data sent from the dispatcher to the consumers.
    - *processResponse*: data from consumers with predictions sent to the dispatcher

2. **Start Elastic Search**:
   Inside `/elasticsearch` folder type ```docker-compose up``` in a new terminal window and hit enter.
   Data will first be indexed once the request is sent to the API, then updated with the respective predictions.
   This is an example of record in Elastic Search:

   | Text | Sentiment |  WordCount |
       | :------- | :------- | :------- |
   | I love it | positive | [{"I" : 1}, {"love" : 1}, {"it" : 1}]  |

## API
The API is built using **Node.js**, you don't need to install anything since it is **fully dockerized**.
Given the request, an **SHA256** is calculated on that in order to use it as the request ID in Elastic Search.
That way, if a duplicated ID already exists we won't have any duplicates.

3. **Start the API**:
   Move to `/api` and type ```docker-compose up``` to run the docker container and start the API.
   You can reach it at localhost:5001.

### Endpoint

| Enpoint | Method |  Body (Parameters) | Description |  
| :------- | :------- | :------- | :------- | 
| **/predict** | *POST* | type, text  | Write the request to kafka general topic |
| **/test** | *GET* |  | Used to test if the API is working correctly |

## Dispatcher
The dispatcher is a **PySpark** machine which reads and analyzes every request sent to the "requests" kafka topic.
Based on the type of the request, it applies some basic **Natural Language Processing** techniques if needed and sends the data to
the `processRequest` topic.

4. **Start the dispatcher**:
   Move to the `/bin` folder and type ```./dispatcher.sh```. This starts a docker container running the dispatcher, Kafka has to be running.

## Machine Learning Consumers
Right now consumers are written in python using the **PySpark** module. They get data from the `processRequest` topic, apply a prediction and send data back to the dispatcher (to the `processResponse` topic).
Two versions of consumers exists: OOP and non-OOP.
Nothing actually changes between the two versions, the first one makes it much easier to implement your custom consumer.

5. **Start a consumer**:
   Move to the `/bin` folder and type ```./sentiment.sh```. This starts a sentiment analysis consumer which will predict the sentiment of the requested
   text using **VADER**.

   **Start an OOP consumer**:
   Move to the `/bin` folder and type ```./soop.sh```. This starts a sentiment analysis oop consumer which will predict the sentiment of the requested
   text using **VADER**.

## Make your own Consumer
### Python and pySpark
If you want to make your own consumer I highly recommend to extend the Consumer class (you can find it in `spark/consumers/Parent/Consumer.py`).
You can also see the `SentimentConsumerOOP` and `WordCountConsumerOOP` examples.

**Things you must do**:
- Pass the type of classification you are applying, for example "Sentiment Analysis" or "Word Count", to the object constructor.
- Define and override a `predict` method, which implements the logic of your classification.
- Make sure your prediction is a string (if it is not, you need to convert it to a string)
- Pass the prediction string to the `toRow` function.
- Return the obtained Row.

### Custom Consumer code example (Python)

```python
from Consumer import Consumer
from pyspark.sql import Row

class CustomConsumer(Consumer):

    def __init__(self, label) -> None:
        super().__init__()
        self.label = label

    def predict(self, data: Row) -> Row:
        """
            Here goes the implementation of your ML algorithm.
        """
        # Just an example
        prediction = MyAlgorithmPrediction()


        return self.toRow(data, prediction)
        
 if __name__ == "__main__":
    customConsumer = WordCountConsumer("typeOfClassification")

    customConsumer.start()

```

### Input
As mentioned before, consumers read data from the **processRequest** kafka topic; you need to de-serialize the `value` attribute into a structure with two fields: **id** and **text**.

Example of a **processRequest** record:

| key | value |
| :-- | :---- |
| not_relevant| "{id:some_id}, {text:text_from_api}" |

### Output
When a consumer has made a prediction, the output must be sent to the **processResponse** kafka topic;

You need to output the following attributes:
- **ID**, the id from the respective input record.
- **type**, a string that indicates the type of your classification.
- **text**, the text on which you made a prediction (maybe it's going to be removed)
- **prediction**, string containing your prediction.

Since the dispatcher expects `key, value`  records, you will need to serialize all of the fields mentioned before in a json string.
Doing so, you should now be able to succesfully send the json string to the **processResponse** topic.

## Java

### Project setup

### Jars

In order to start everything you need to build fat jars for each project (maybe there is a better way?).
To do this in IntelliJ Idea do the following:
1. File -> Project Structure
2. Click the plus symbol -> JAR -> From module with dependencies...
3. Select `Copy to the output directory and link via manifest`
4. Change the jar `Name` to the module name followed by :jar
5. Change the output path to `path/to/module/out/artifacts/module_name`

Repeat for each submodule, you should obtain something like this:
![Jars](docs/jars.PNG)

## How to run SAPERE

Make sure to do the following before you start SAPERE locally (using IntelliJ Idea):
1. Go to Build -> Build Artifacts... -> All Artifacts -> Clean
2. In the root folder type `mvn clean package`
3. Go to Build -> Build Artifacts... -> All Artifacts -> Build

![Project Tree](docs/tree.PNG)

Now you can start everything, but make sure to do it properly:
0. Create a new docker network
1. Start Kafka and Zookeeper: `./bin/kafka.sh`
2. Start Elastic Search and Kibana:`./bin/elastic.sh`
3. Start Hadoop and HDFS: `./bin/hadoop.sh`
4. Start the APIS: `./bin/api.sh`
5. Start the Dispatcher: `./bin/dispatcher.sh`
6. Start Jero consumer: `./bin/consumer.sh`