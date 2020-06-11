"""
Scraper wide further developments:
    * TODO: Use Selenium to scrape only previously unseen houses (based on new urls?), use standard requests for other
    houses. Selenium is really slow (5-10s / it), but it is necessary to load neighbourhood statistics from the page
    using JS.

    * TODO: Add date of scraping as a variable.

    * TODO: Optimize for memory usage to be able to use smaller instances.

    * TODO: Run this on the start of the "scraper" VM. Docker maybe?

"""
import os
import random
from subprocess import Popen

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent

import re
import string
import unidecode

import pandas as pd
import json

import time
import tqdm


class Scraper:
    def __init__(self, logger, max_retries=3, verbose=True):
        """
        Class that scrapes the given website. Use the scraping method is Scraper().scrape.

        Parameters
        ----------
        max_retries (int): Number of retries to reset the Tor connections, selenium browsers etc.
        verbose (bool): Whether to display a scraping progress bar.
        """

        # Initialize class variables.
        self.max_retries = max_retries
        self.verbose = verbose
        self.logger = logger

        # Load the config files.
        with open("config/config_scraper.json") as f:
            self.config = json.load(f)

        # Set chrome options to be able to run in docker.
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--window-size=1420,1080')
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--disable-gpu')

        # Get initial proxied session.
        self.session = self.get_proxy_session()
        pass

    def get_proxy(self):
        """
        TODO: This piece of shit service doesn't work. Will use a random list for now. Get a premium service api in
         the future.
        Gets a proxy using proxyscrape.

        Current country codes used: lv, lt, pl, ee, de

        Returns
        -------
        proxy (str): Proxy str in the format of host:port.
        """

        proxy_list = [
            "163.172.180.18:8811",
            "37.123.164.8:8888",
            "94.245.105.90:80"
        ]

        proxy_number = random.randint(0, len(proxy_list) - 1)
        return proxy_list[proxy_number]

    def get_proxy_session(self):
        """
        Creates a requests.session object using proxy ports. Automatically switches to a different proxy in case of an
        Exception.

        Returns
        -------
        requests.session object
        """
        # TODO: Get proxied session.

        correct_output = False
        retries = 0
        while (not correct_output) and (retries < self.max_retries):
            try:
                # Build a proxy session.
                proxy = self.get_proxy()
                session = requests.session()
                session.proxies = {
                    'http': proxy,
                    'https': proxy
                }

                # Set additional Tor session parameters.
                session.headers = UserAgent().random

                # Check if the connection works.
                output = session.get('http://httpbin.org/ip')

                # If no ProxyError occurs, report number of proxies and the connected ip.
                self.logger.info('Built a session at IP {}.'.format(output.json()['origin']))
                correct_output = True

            except Exception as e:
                # Retry in the case of a failed proxy.
                self.logger.warning(
                    'Proxy at {} failed. Trying a different one.'.format(proxy))
                self.logger.warning(e)
                retries += 1

        return session

    def get_tor_session(self):
        """
        Note: Depreciated function. Requires standalone tor files / modifying to be able to use.

        Creates a requests.session object using Tor socks5 proxie ports.

        Returns
        -------
        requests.session object
        """

        # Use Tor ports for a requests.session.
        session = requests.session()
        session.proxies = {
            'http': 'socks5://127.0.0.1:9050',
            'https': 'socks5://127.0.0.1:9050'
        }

        # Set additional Tor session parameters.
        session.headers = UserAgent().random

        self.logger.info('Built a Tor session at IP {}.'.format(session.get('http://httpbin.org/ip').json()['origin']))
        return session

    def restart_tor(self):
        """
        Note: Depreciated function. Requires standalone tor files / modifying to be able to use.

        Restarts Tor to generate a new IP address.

        A forceful hack since Tor doesn't function as intended on my windows and none of the solution work.
        Too bad! https://www.youtube.com/watch?v=k238XpMMn38

        Returns
        -------
        requests.session object
        """

        # Kill existing tor process.
        os.system("taskkill /F /im tor.exe")

        # Run a tor process in the background, wait for it to start.
        tor_path = os.getcwd() + self.config['file_paths']['tor']
        Popen(tor_path, shell=True)

        time.sleep(1)
        return self.get_tor_session()

    def get_number_of_pages(self, url):
        """
        Gets the total number of pages using the page button numeration at the end of the page.

        Automatically restarts in the case of failing to get the data.


        Parameters
        ----------
        url (str): Main page url that has the button numeration.

        Returns
        -------
        (int):  Number of total pages.

        Raises
        ------
        TimeoutError: In the case of retries exceeding self.max_retries.
        """

        # Error handling with limited retries. Will log error messages and restart the Tor connection, assuming it was
        # banned. Otherwise will continue.
        correct_output = False
        retries = 0
        while (not correct_output) and (retries < self.max_retries):
            try:
                # Scrape the main page to get the total number of pages.
                page = self.session.get(url)
                soup = BeautifulSoup(page.content, 'html.parser')

                # Extract all page number buttons to find the maximum digit value.
                page_number_buttons = str(soup.find_all(class_=self.config['html_tags']['page_number_button']))
                page_number_buttons = re.findall('\d+', page_number_buttons)

                total_pages = max([int(number) for number in page_number_buttons])

                correct_output = True
                return total_pages

            except Exception as e:
                self.logger.warning('Exception occurred at get_number_of_pages:')
                self.logger.warning(e)

                retries += 1
                self.session = self.get_proxy_session()

        error_message = 'Max retries exceeded with url {}.'.format(url)
        self.logger.error(error_message)
        raise TimeoutError(error_message)

    def get_page_urls(self, url):
        """
        Gets all the listing_urls' urls on the given page.


        Parameters
        ----------
        url (str): page that will be searched on.

        Returns
        -------
        (list): Listing urls found in the given page.

        Raises
        ------
        TimeoutError: In the case of retries exceeding self.max_retries.
        """

        correct_output = False
        retries = 0
        while (not correct_output) and (retries < self.max_retries):
            try:
                # Get page contents and put them in a "soup".
                page = self.session.get(url)
                soup = BeautifulSoup(page.content, 'html.parser')

                # Extract all listings and get their urls.
                listings = soup.find_all(class_=self.config['html_tags']['listing_url'])
                listings_urls = [listing.find('a', href=True)['href'] for listing in listings]

                correct_output = True
                return listings_urls

            except Exception as e:
                self.logger.warning('Exception occurred at get_page_urls:')
                self.logger.warning(e)

                retries += 1
                self.session = self.get_proxy_session()

        error_message = 'Max retries exceeded with url {}.'.format(url)
        self.logger.error(error_message)
        raise TimeoutError(error_message)

    def get_urls(self):
        """
        Gets all of the listing urls from all of the pages of the website. Combines get_number_of_pages and
        get_page_urls methods.


        Returns
        -------
        (list): List of all the urls on the website.
        """

        total_pages = self.get_number_of_pages(self.config['urls']['main'])

        # Get listing urls for all of the pages.
        listing_urls = []
        for page_number in range(1, total_pages + 1):
            page_url = self.config['urls']['listings'] + str(page_number) + self.config['urls']['listings_settings']

            page_urls = self.get_page_urls(page_url)
            listing_urls.extend(page_urls)

        # There are urls that are auto-generated with each page visit, possibly honeypots for scraper catchers.
        listing_urls = [url for url in listing_urls if self.config['urls']['honeypot'] not in url]

        return listing_urls

    def parse_object_data(self, url):
        """
        Scrapes and parses the object data for the given url.

        Handles bans by automatically restarting the tor connection.


        Parameters
        ----------
        url (str): Page url to be scraped.

        Returns
        -------
        (dict): Object data found in the given url.

        Raises
        ------
        TimeoutError: In the case of retries exceeding self.max_retries while restarting in the case of a ban.
        """

        # Get page source data, parse into a soup.
        self.driver.get(url)
        page_source = self.driver.page_source

        soup = BeautifulSoup(page_source, 'lxml')

        # TODO: Add ban check using the soup here, once you get banned. This might not ever happen, because monkeys.
        def ban_check(page_soup):
            ban = False
            # Logs the ban.
            if ban:
                self.logger.warning('Banned with existing Tor connection.')

            return False

        # If the driver got banned, resets everything and reloads the page until it works or self.max_retries exceeded.
        retries = 0
        banned = ban_check(soup)
        while banned:
            # Restart the session and the driver.
            self.session = self.get_proxy_session()
            self.driver.quit()
            self.driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=self.chrome_options)

            # Reload the page and recheck if it's still banned.
            self.driver.get(url)
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'lxml')
            banned = ban_check(soup)

            # Raise TimeoutError if retries >= self.max_retries.
            retries += 1
            if retries >= self.max_retries:
                error_message = 'Max retries exceeded with url {}.'.format(url)
                self.logger.error(error_message)
                raise TimeoutError(error_message)

        # Find all name and item classes within object details class. Multiple tag values in config are necessary
        # because this website was built by monkeys.
        object_details_class = soup.find(class_=self.config['html_tags']['object_details'])

        # These either contain dead urls or scraper honeypot urls that might potentially cause bans on visit.
        if object_details_class is None:
            self.logger.info('Found a dead / scraper catcher url. {}'.format(url))
            return None

        object_names = object_details_class.find_all(self.config['html_tags']['object_details_names'])
        object_items = object_details_class.find_all(self.config['html_tags']['object_details_items'])

        # Get field names.
        object_names = [item.contents[0] for item in object_names]

        # Extract values from item fields.
        object_item_values = []
        for object_item in object_items:

            # Single value items (e.g. Price, SqMeters).
            if len(object_item) == 1:
                object_item_values.append(object_item.contents[0])

            # Multiple value items (e.g. list of ExtraFeatures, list of Security).
            elif len(object_item) > 1:

                # Tries to get list-like items.
                items = [item.contents[0] for item in
                         object_item.find_all(class_=self.config['html_tags']['object_details_items_separator'])]
                if len(items) != 0:
                    object_item_values.append(items)

                # If the list is empty, the item is actually a single value item, with additional info -
                # ads, links, etc.
                else:
                    object_item_values.append(object_item.contents[0])

        # Set everything as a dictionary.
        object_data = dict(zip(object_names, object_item_values))

        # Get object description text and listing name - City, Neighbourhood, Street, Other. Store Object Url.
        object_data['Object Description'] = soup.find(id=self.config['html_tags']['object_description']).contents
        object_data['Listing Name'] = soup.find(class_=self.config['html_tags']['object_name']).contents[0]
        object_data['Listing Url'] = url

        # Get object listing statistics (views, favorites, etc.).
        listing_statistics = soup.find(class_=self.config['html_tags']['listing_statistics'])

        # Sometimes both Views and Favorites are missing, sometimes only the Favorites.
        if listing_statistics is not None:
            object_data[listing_statistics.contents[0]] = listing_statistics.contents[1].contents[0]
        elif (listing_statistics is not None) and (len(listing_statistics.contents) > 2):
            object_data['Listing Favorites'] = listing_statistics.contents[3].contents[0]

        # Try to find realtor name and realtor organization. Both are optional variables, both can be present at the
        # same time. Sometimes, realtor name field is present but empty.
        realtor_name = soup.find(class_=self.config['html_tags']['realtor_name'])
        realtor_organization = soup.find(class_=self.config['html_tags']['realtor_organization'])

        # Lots of specific exceptions due to monkey code. Don't try to uderstand and, not, etc. statemets.
        # It works as intended and you should be happy about it.
        if (realtor_name is not None) and (len(realtor_name) != 0) and (not ('Pardavėjo kontaktai' in realtor_name.contents[0]) and not ('Nuomotojo kontaktai' in realtor_name.contents[0])):
            object_data['Realtor Name'] = realtor_name.contents[0]
            object_data['Realtor'] = 1

        if realtor_organization is not None:
            object_data['Realtor Organization'] = realtor_organization.contents[1]['href']

        # TODO: When re-scraping  the data without selenium, this part becomes optional.
        # Find all name and item classes withing object details class.
        neighbourhood_statistics = soup.find(id=self.config['html_tags']['neighbourhood_statistics'])

        neighbourhood_statistics_names = neighbourhood_statistics.find_all(
            class_=self.config['html_tags']['neighbourhood_statistics_names'])
        neighbourhood_statistics_items = neighbourhood_statistics.find_all(
            class_=self.config['html_tags']['neighbourhood_statistics_items'])

        # Extract the name and the item values, add them to the dictionary.
        neighbourhood_statistics_names = [item.contents[0] for item in neighbourhood_statistics_names]
        neighbourhood_statistics_items = [item.contents[0] for item in neighbourhood_statistics_items]

        for name, item in zip(neighbourhood_statistics_names, neighbourhood_statistics_items):
            object_data[name] = item

        # TODO: When re-scraping  the data without selenium, this part becomes optional.
        # Get energy class rating.
        building_energy_class = soup.find(class_=self.config['html_tags']['building_energy_class'])

        if building_energy_class is not None:
            object_data['Building Energy Class'] = building_energy_class.contents[1].contents[0]
            object_data['Building Energy Class Category'] = building_energy_class.contents[2]
        else:
            object_data['Building Energy Class'] = None
            object_data['Building Energy Class Category'] = None

        return object_data

    def process_object_data(self, data):
        """
        Processes the object data of the given dictionary.

        Sets the correct formats, processes strings into readable formats, creates categorical and pseudo-categorical
        variables.

        TODO: Fix all the bugs, make most of the parameters optional.


        Parameters
        ----------
        data (dict): Dictionary to process.

        Returns
        -------
        object_data (dict): Processed dictionary.
        """

        variables_integer = ['Plotas', 'KainaMėn', 'KambariųSk', 'Aukštas', 'AukštųSk', 'Metai', 'ArtimiausiasDarželis',
                             'ArtimiausiaMokymoĮstaiga', 'ArtimiausiaParduotuvė', 'ViešojoTransportoStotelė',
                             'Nusikaltimai500MSpinduliuPraėjusįMėnesį']
        variables_categorical = ['PastatoTipas', 'Šildymas', 'Įrengimas', 'NamoNumeris',
                                 'PastatoEnergijosSuvartojimoKlasė', 'ButoNumeris', 'BuildingEnergyClassCategory',
                                 'VidutiniškaiTiekKainuotųŠildymas1Mėn']
        variables_lists = ['Ypatybės', 'PapildomosPatalpos', 'PapildomaĮranga', 'Apsauga']
        variables_drop = []

        # Transform keys from whatever messy format to VariableName.
        keys = list(data.keys())
        keys = [key.strip() for key in keys]
        keys = [key.translate(str.maketrans('', '', string.punctuation)) for key in keys]
        keys = [''.join(key.lower().title().split(' ')) for key in keys]

        object_data = dict(zip(keys, list(data.values())))

        # Convert integer variables to integer.
        for variable in variables_integer:
            # Handles cases when variable is missing:
            try:
                object_data[variable] = int(re.findall('\d+', object_data[variable])[0])
            except KeyError:
                continue

        # Remove whitespace from categorical variables.
        for variable in variables_categorical:
            # Handles cases when variable is missing:
            try:
                object_data[variable] = object_data[variable].strip()
            except (KeyError, AttributeError):
                continue

        # Transform lists of variables into pseudo categorical variables.
        # E.g. Variable: ['feat1', 'feat2'] -> Variable_feat1: 1, Variable_feat2: 1.
        for variable in variables_lists:
            try:
                # Convert space delimited text to TitleCamelCase. E.g. 'This house' -> 'ThisHouse'.
                object_data[variable] = [''.join(item.title().split(' ')) for item in object_data[variable]]

                # Create a pseudo categorical variable where multiple values in a category is present.
                for feature in object_data[variable]:
                    object_data[variable + '_' + feature] = 1

                variables_drop.append(variable)
            except KeyError:
                continue

        # Split ListingName into City, Neighbourhood, Street.
        object_data['ListingName'] = object_data['ListingName'].split(',')
        object_data['BuildingCity'] = object_data['ListingName'][0].strip()
        object_data['BuildingNeighbourhood'] = object_data['ListingName'][1].strip()
        object_data['BuildingStreet'] = object_data['ListingName'][2].strip()

        # Transform ObjectDescription into a single string, as well as replacing <br/>'s with \n.
        object_data['ObjectDescription'] = ''.join([str(item) for item in object_data['ObjectDescription']])
        object_data['ObjectDescription'] = re.sub('<br/s*?>', '\n', object_data['ObjectDescription'])

        # Split Total/Today views into separate variables.
        if 'SkelbimąPeržiūrėjoIšVisošiandien' in list(object_data.keys()):
            object_data['SkelbimąPeržiūrėjoIšVisošiandien'] = object_data['SkelbimąPeržiūrėjoIšVisošiandien'].split('/')
            object_data['ListingViewsTotal'] = int(object_data['SkelbimąPeržiūrėjoIšVisošiandien'][0])
            object_data['ListingViewsToday'] = int(object_data['SkelbimąPeržiūrėjoIšVisošiandien'][1])
            variables_drop.append('SkelbimąPeržiūrėjoIšVisošiandien')

        # Transform "\n         69,420 €/mėn." format into a float.
        if 'VidutiniškaiTiekKainuotųŠildymas1Mėn' in list(object_data.keys()):
            heating = object_data['VidutiniškaiTiekKainuotųŠildymas1Mėn'].strip()
            heating = re.findall('\d+,\d+', heating)[0]
            object_data['VidutiniškaiTiekKainuotųŠildymas1Mėn'] = float(heating.replace(',', '.'))

            # Drop no longer required variables.
        variables_drop.extend(['ListingName'])
        for variable in variables_drop:
            object_data.pop(variable)

        # Replace all Lithuanian characters in field names with non Lithuanian due to google cloud errors.
        object_data = dict((unidecode.unidecode(key), value) for (key, value) in object_data.items())

        return object_data

    def get_object_data(self, listing_urls):
        """
        Gets object data for all of the urls in listing_urls.

        Automatically restarts selenium in the case of a crash.


        Parameters
        ----------
        listing_urls (list): Urls to get the object data from.

        Returns
        -------
        (list): Object data for each of the given urls.

        Raises
        ------
        TimeoutError: In the case of retries exceeding self.max_retries while restarting selenium.
        """

        # Optional parameter to display a progress bar.
        if self.verbose:
            loop = tqdm.tqdm(listing_urls[:20])  # TODO: TEMPORARY FOR TESTING.
        else:
            loop = listing_urls

        data = pd.DataFrame()
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=self.chrome_options)
        for listing_url in loop:

            # Restarts selenium if it crashes (which happen quite often).
            correct_output = False
            retries = 0
            while not correct_output:
                try:
                    listing_data = self.parse_object_data(listing_url)

                    if listing_data is not None:
                        data = data.append(pd.Series(self.process_object_data(listing_data)), ignore_index=True)

                    correct_output = True
                    continue

                except Exception as e:
                    self.logger.warning('Exception occurred at parse_object_data:')
                    self.logger.warning(e)

                    # Restart the driver.
                    self.driver.quit()
                    self.driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=self.chrome_options)

                    # Raise TimeoutError if retries >= self.max_retries.
                    retries += 1
                    if retries >= self.max_retries:
                        error_message = 'Max retries exceeded with url {}.'.format(listing_url)
                        self.logger.error(error_message)
                        raise TimeoutError(error_message)

        self.driver.quit()
        return data

    def scrape(self):
        """
        Main method of scraping combining all of the methods within the class.

        Uses the config_scraper.json configuration to scrape the given website.

        TODO: Object description might have ,'s and "'s, which make saving to csv dangerous. Figure out how to fix it.

        Returns
        -------
        pandas.DataFrame: Data containing all of the processed-raw (none of the information removed) lissting data from
        the website.
        """

        self.logger.info('Getting the urls.')
        listing_urls = self.get_urls()
        self.logger.info('Getting the urls was successful.')

        self.logger.info('Getting and parsing the object data.')
        df = self.get_object_data(listing_urls)
        self.logger.info('Getting and parsing the object data was successful, returning the DataFrame.')

        return df
