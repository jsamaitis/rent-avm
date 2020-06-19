from scraper import Scraper
from format_verifier import FormatVerifier
from uploader_cleaner import UploaderCleaner

import json

from googleapiclient import discovery
import logging
import google.cloud.logging


# Setup Logging.
client = google.cloud.logging.Client()
client.get_default_handler()
client.setup_logging()


def scrape():
    """Runs scraper, format_verifier, uploader_cleaner code on Google Cloud VM."""

    logging.info('Started the scraping process.')
    scraper = Scraper(logger=logging)
    verifier = FormatVerifier(logger=logging)
    uploader_cleaner = UploaderCleaner(logger=logging)

    # Scrape, clean & upload, verify the data.
    df = scraper.scrape()
    uploader_cleaner.etl(df)
    verifier.verify(df)
    return json.dumps({'message': "Successfully scraped, uploaded and verified the data."})


def stop_vm(instance_name='scraper', instance_zone='europe-north1-a', project_id='rent-avm'):
    """
    Method used to stop a VM instance on Google Cloud Platform with the given parameters.

    Parameters
    ----------
    instance_name (str) : VM Instance name.
    instance_zone (str) :  VM Instance zone.
    project_id (str) : Id of the project.

    Returns
    -------
    response (json) : Output of the execution of instances.stop().
    """

    service = discovery.build('compute', 'v1')
    request = service.instances().stop(instance=instance_name, zone=instance_zone, project=project_id)
    response = request.execute()  # This can be returned if necessary.

    logging.info('Shut down the scraper VM.')
    return json.dumps(response)


if __name__ == '__main__':
    scrape()
    stop_vm()
