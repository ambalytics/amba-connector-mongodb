from kafka import KafkaConsumer, KafkaProducer
import pymongo
import logging
import json


def main(config):
    logging.warning('MongoDB topic %r in collection %s, started' % (
        config['topic'], config['mongo_collection']))
    mongoClient = pymongo.MongoClient(config['mongo_url'])
    db = mongoClient[config['mongo_client']]
    collection = db[config['mongo_collection']]

    consumer = KafkaConsumer(config['topic'], group_id=config['group_id'],
                             bootstrap_servers=config['bootstrap_servers'], api_version=(0, 10),
                             consumer_timeout_ms=5000)

    while True:
        try:
            for msg in consumer:
                logging.warning('MongoDB topic %r in collection %s, write' % (
                    config['topic'], config['mongo_collection']))
                json_response = json.loads(msg.value)
                collection.insert_one(json_response)
        except pymongo.errors.DuplicateKeyError:
            logging.warning("MongoDB topic %r in collection %s, Duplicate found, continue" % (
            config['topic'], config['mongo_collection']))
        except Exception as exc:
            consumer.close()
            logging.error('MongoDB topic %r in collection %s, generated an exception: %s' % (
            config['topic'], config['mongo_collection'], exc))
            break

if __name__ == '__main__':
    config = {
        'mongo_url': "mongodb://localhost:27017/",
        'mongo_client': "TweetTest",
        'mongo_collection': "test_link",
        'topic': 'linked',
        'group_id': 'mongo-db-consumer-linked',
        'bootstrap_servers': 'localhost:9092',
    }
    main(config=config)