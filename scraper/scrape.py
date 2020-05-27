from scraper import Scraper
from format_verifier import FormatVerifier

scraper = Scraper()
verifier = FormatVerifier()

df = scraper.scrape()
df.to_csv('../data/raw_listings.csv', index=False)

verifier.verify(df)

