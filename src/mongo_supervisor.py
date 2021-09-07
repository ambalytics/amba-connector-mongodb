import signal
import time
from multiprocessing import Process
import logging

from mongodb_connector import MongoDBConnector


class Supervisor:
    """supervisor class
    """

    def __init__(self):
        """init"""
        self.running = True
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)  # need
        self.workers = list()
        self.tw = list()

    def stop(self, signum, els):
        """stop"""
        logging.warning("Supervisor stop")
        self.stop_workers()
        self.running = False

    def stop_workers(self):
        """stop worker"""
        logging.warning("Supervisor    : close threads.")

        for tw in self.tw:
            tw.stop()

        for work in self.workers:
            work.close()

    def main(self):
        """setup logging """
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO,
                            datefmt="%H:%M:%S")


if __name__ == "__main__":
    logging.warning('start mongo worker ... connect in %s' % '10')

    time.sleep(10)

    supervisor = Supervisor()
    supervisor.main()

    total_workers = 0
    states = ['unlinked', 'linked', 'unknown', 'processed', 'aggregated']
    print(states)

    for state in states:
        logging.warning("Main    : create and start thread %d with state %s" % (total_workers, state))
        t = MongoDBConnector(total_workers)
        t.set_state(state)
        supervisor.tw.append(t)
        worker = Process(target=t.consume)
        worker.start()
        supervisor.workers.append(worker)
        total_workers += 1
