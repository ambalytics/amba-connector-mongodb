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
        # todo client for each thread
        self.mongoClient = pymongo.MongoClient(host=self.config['mongo_url'],
                                               serverSelectionTimeoutMS=3000,  # 3 second timeout
                                               username="root",
                                               password="example"
                                               )
        self.db = self.mongoClient[self.config['mongo_client']]
        self.collection = self.db[self.config['mongo_collection']]

    def set_state(self, state):
        self.state = [state]
        self.group_id = "mongo_db_connector_lpd-" + state

    def on_message(self, json_msg):
        logging.warning(self.log + "insert %s in collection %s " % (json_msg['id'], json_msg['state']))
        # logging.warning(json_msg)

        # todo use id from event
        json_msg['_id'] = json_msg['id']

        try:
            # self.collection.insert_one(json_msg)
            self.db[json_msg['state']].insert_one(json_msg)
        except pymongo.errors.DuplicateKeyError:
            logging.warning("MongoDB collection/state%s, Duplicate found, continue" % json_msg['state'])


if __name__ == '__main__':
    logging.warning("start Mongo DB connector")
    time.sleep(15)

    e = MongoDBConnector(1)
    e.consume()
