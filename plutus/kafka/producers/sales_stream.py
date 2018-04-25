from confluent_kafka import Producer
import numpy as np
import argparse
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)

logger.addHandler(sh)


class SalesEventGenerator(object):
    DEVICE_CATEGORY = ["web", "ios", "android"]
    PRODUCT_CATEGORY = ["home", "electronics", "automobile"]
    MAX_AMT = 1000

    def generate(self) -> dict:
        """
        Generate a sales event
        :return:
        """
        return {
            "device_category": np.random.choice(self.DEVICE_CATEGORY),
            "product_category": np.random.choice(self.PRODUCT_CATEGORY),
            "amount": np.random.uniform(low=0, high=self.MAX_AMT)
        }


def acked(err, msg):
    """
    Callback invoked by kafka producer
    :param err:
    :param msg:
    :return:
    """
    if err is not None:
        logger.info(
            "Failed to deliver message: {value}: {err_msg}".format(
                value=msg.value(),
                err_msg=msg
            ))
    else:
        logger.info(
            "Message produced: {msg}".format(msg=msg.value())
        )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--topic", "-t", dest="topic",
                        help="kafka topic", required=True)
    parser.add_argument("--bootstrap-server", "-s", dest="bootstrap_server",
                        help="bootstrap server", required=True)
    cli_args = parser.parse_args()
    topic = cli_args.topic
    bserver = cli_args.bootstrap_server
    producer_params = {
        "bootstrap.servers": bserver
    }
    p = Producer(**producer_params)
    sg = SalesEventGenerator()
    try:
        while True:
            p.produce(topic, key="device_category",
                      value=json.dumps(sg.generate()).encode("utf-8"),
                      callback=acked)
            p.poll(0.5)
    except KeyboardInterrupt:
        p.flush()


if __name__ == "__main__":
    main()
