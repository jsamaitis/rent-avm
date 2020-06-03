from scraper import Scraper
from format_verifier import FormatVerifier
from flask import Flask, request


if __name__ == '__main__':

    app = Flask(__name__)


    @app.route('/scrape', methods=['POST', 'GET'])
    def scrape():
        scraper = Scraper()
        verifier = FormatVerifier()

        df = scraper.scrape()
        # TODO: Save daily data, if_exists=replace. Add timestamp. Ways to automatically add column names?
        df.to_gbq('rent_avm.raw_listings', project_id='rent-avm', if_exists='replace', progress_bar=False)

        verifier.verify(df)
        return 'Success'

    app.run(host='localhost', port=8080, debug=False)