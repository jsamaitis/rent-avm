from scraper import Scraper
from format_verifier import FormatVerifier

from flask import Flask

import datetime
import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging


# Setup Logging.
# TODO: All logs appear as "default" instead of warning, info etc. Works when the scope is Global.
client = google.cloud.logging.Client()
client.get_default_handler()
client.setup_logging()

app = Flask(__name__)


@app.route('/', methods=['POST', 'GET'])
def scrape():
    logging.info('Started the scraping process.')
    scraper = Scraper(logger=logging)
    verifier = FormatVerifier(logger=logging)

    df = scraper.scrape()
    # TODO: Ways to automatically add column names in order to append instead of daily saving?
    #  Otherwise create a separate GFunction to clear and merge data.
    table_name = 'rent_avm.raw_listings_{}'.format(datetime.datetime.today().date().strftime('%Y-%m-%d'))
    df.to_gbq(table_name, project_id='rent-avm', if_exists='replace', progress_bar=False)

    verifier.verify(df)
    return 'Successfully Scraped'


if __name__ == '__main__':
    app.run()
