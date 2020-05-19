from scraper import Scraper

scraper = Scraper()

df = scraper.scrape()
df.to_csv('../data/raw_listings.csv', index=False)