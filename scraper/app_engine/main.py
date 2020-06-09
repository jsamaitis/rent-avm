from flask import Flask
from googleapiclient import discovery

import logging
import google.cloud.logging

import json


# Setup Logging.
# TODO: All logs appear as "default" instead of warning, info etc. Works when the scope is Global.
client = google.cloud.logging.Client()
client.get_default_handler()
client.setup_logging()

app = Flask(__name__)

# TODO: CLEAN THIS SHIT UP.
@app.route('/', methods=['POST', 'GET'])
def start_vm():
    """
    TODO: Descr.
    Returns
    -------

    """

    service = discovery.build('compute', 'v1')
    request = service.instances().start(instance='scraper', zone='us-central1-a', project='rent-avm')
    response = request.execute()

    logging.info('Started the scraper VM.')
    return json.dumps(response, indent=4)



if __name__ == '__main__':
    app.run()
