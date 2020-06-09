from scraper import Scraper
from format_verifier import FormatVerifier

import datetime
import logging
import google.cloud.logging


# Setup Logging.
client = google.cloud.logging.Client()
client.get_default_handler()
client.setup_logging()


def scrape():
    logging.info('Started the scraping process.')
    scraper = Scraper(logger=logging)
    verifier = FormatVerifier(logger=logging)

    df = scraper.scrape()
    # TODO: Ways to automatically add column names in order to append instead of daily saving?
    #  Otherwise create a separate GFunction to clear and merge data.
    table_name = 'data_listings.raw_listings_{}'.format(datetime.datetime.today().date().strftime('%Y_%m_%d'))
    df.to_gbq(table_name, project_id='rent-avm', if_exists='replace', progress_bar=False)

    verifier.verify(df)
    return 'Successfully Scraped.'


if __name__ == '__main__':
    scrape()
