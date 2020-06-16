from flask import Flask
from googleapiclient import discovery

import logging
import google.cloud.logging

import json


# Setup Logging.
client = google.cloud.logging.Client()
client.get_default_handler()
client.setup_logging()

app = Flask(__name__)


@app.route('/', methods=['POST', 'GET'])
def start_vm(instance_name='scraper', instance_zone='europe-north1-a', project_id='rent-avm'):
    """
    Method used to start a VM instance on Google Cloud Platform with the given parameters.

    Parameters
    ----------
    instance_name (str) : VM Instance name.
    instance_zone (str) :  VM Instance zone.
    project_id (str) : Id of the project.

    Returns
    -------
    response (json) : Output of the execution of instances.start().
    """
    service = discovery.build('compute', 'v1')
    request = service.instances().start(instance=instance_name, zone=instance_zone, project=project_id)
    response = request.execute()

    logging.info('Started the "{}" VM.'.format(instance_name))
    return json.dumps(response)


if __name__ == '__main__':
    app.run()
