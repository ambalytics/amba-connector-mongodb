import time

import pymongo
import logging

from event_stream.event_stream_consumer import EventStreamConsumer


class MongoDBConnector(EventStreamConsumer):
    state = ["linked"]
    relation_type = "discusses"

    log = "MongoDB Connector "
    group_id = "mongo_db_connector_lpd"

    mongoClient = False

    config = {
        'mongo_url': "mongodb://mongo_db:27017/",
        'mongo_client': "events",
        'mongo_collection': "dev",
    }

    def __init__(self, id):
        super().__init__(id)
        # todo
        self.mongoClient = pymongo.MongoClient(host=self.config['mongo_url'],
                                               serverSelectionTimeoutMS=3000,  # 3 second timeout
                                               # username="root",
                                               # password="example"
                                               )
        db = self.mongoClient[self.config['mongo_client']]
        self.collection = db[self.config['mongo_collection']]

    def on_message(self, json_msg):
        logging.warning(self.log + "on message mongo consumer")

        try:
            self.collection.insert_one(json_msg)
        except pymongo.errors.DuplicateKeyError:
            logging.warning("MongoDB topic %r in collection %s, Duplicate found, continue" % (
                self.config['topic'], self.config['mongo_collection']))


if __name__ == '__main__':
    logging.warning("start Mongo DB connector")
    time.sleep(15)

    e = MongoDBConnector(1)
    e.consume()
