import time
import pymongo
import logging

from event_stream.event_stream_consumer import EventStreamConsumer


class MongoDBConnector(EventStreamConsumer):
    """save events from kafka into a mongo db """
    state = ["linked"]
    relation_type = "discusses"

    log = "MongoDB Connector "
    group_id = "mongo_db_connector_lpd"

    mongo_client = False

    config = {
        'mongo_url': "mongodb://mongo_db:27017/",
        'mongo_client': "events",
        'mongo_collection': "dev",
    }

    def __init__(self, id):
        """init

        Arguments:
            id: the id
        """
        super().__init__(id)
        # todo client for each thread
        self.mongo_client = pymongo.MongoClient(host=self.config['mongo_url'],
                                               serverSelectionTimeoutMS=3000,  # 3 second timeout
                                               username="root",
                                               password="example"
                                               )
        self.db = self.mongo_client[self.config['mongo_client']]
        self.collection = self.db[self.config['mongo_collection']]

    def set_state(self, state):
        """set status

        Arguments:
            state: state to set
        """
        self.state = [state]
        self.group_id = "mongo_db_connector_lpd-" + state

    def on_message(self, json_msg):
        """save the json_msg in mongo

        Arguments:
            json_msg: the event json to save
        """
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
