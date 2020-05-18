import os
import time
from subprocess import Popen

import requests
from bs4 import BeautifulSoup
from selenium import webdriver

import re
import string

from fake_useragent import UserAgent

import pandas as pd

import logging


class Scraper():
    def __init__(self, max_retries=3):
        '''
        TODO Descr.
        '''

        self.max_retries = max_retries


        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler("debug.log"),
                logging.StreamHandler()
            ]
        )

        self.session = self.get_tor_session()
        pass

    def get_tor_session(self):
        '''
        TODO: Descr.

        :return:
        '''
        # Use Tor ports for a requests.session.
        session = requests.session()
        session.proxies = {
            'http': 'socks5://127.0.0.1:9050',
            'https': 'socks5://127.0.0.1:9050'
        }

        # Set additional Tor session parameters.
        session.headers = UserAgent().random

        logging.info('Built a Tor session at IP {}.'.format(session.get('http://httpbin.org/ip').json()['origin']))
        return session


    def restart_tor(self):
        '''
        A forceful hack since tor doesn't function as intended on my windows and none of the solution work. Too bad! https://www.youtube.com/watch?v=k238XpMMn38

        Commented out code is how it should work.

        TODO: Descr.
        '''
        tor_path = os.getcwd() + '/tor-win32-0.4.2.7/Tor/tor.exe'

        # Kill existing tor process. TODO: Make this OS-proof.
        os.system("taskkill /F /im tor.exe")

        # Run a tor process in the background, wait for it to start.
        Popen(tor_path, shell=True)

        time.sleep(1)

        return self.get_tor_session()


    def get_number_of_pages(self, url):
        '''
        TODO: Descr.
        '''

        # Error handling with limited retries. Will log error messages and restart the Tor connection, assuming it was
        # banned. Otherwise will continue.
        correct_output = False
        retries = 0
        while (correct_output) or (retries < self.max_retries):
            try:
                # Scrape the main page to get the total number of pages.
                page = self.session.get(url)
                soup = BeautifulSoup(page.content, 'html.parser')

                # Extract all page number buttons to find the maximum digit value.
                page_number_buttons = str(soup.find_all(class_='page-bt'))
                page_number_buttons = re.findall('\d+', page_number_buttons)

                total_pages = max([int(number) for number in page_number_buttons])

                correct_output = True
                return total_pages

            except Exception as e:
                logging.warning('Exception occurred at get_number_of_pages:')
                logging.warning(e)

                retries += 1
                self.session = self.restart_tor()

        error_message = 'Max retries exceeded with url {}.'.format(url)
        logging.error(error_message)
        raise TimeoutError(error_message)


    def get_page_urls(self, url):
        '''
        TODO: Descr.
        '''
        correct_output = False
        retries = 0
        while (correct_output) or (retries < self.max_retries):
            try:
                # Get page contents and put them in a "soup".
                page = self.session.get(url)
                soup = BeautifulSoup(page.content, 'html.parser')

                # Extract all listings and get their urls.
                listings = soup.find_all(class_='list-adress')
                listings_urls = [listing.find('a', href=True)['href'] for listing in listings]

                correct_output = True
                return listings_urls

            except Exception as e:
                logging.warning('Exception occurred at get_page_urls:')
                logging.warning(e)

                retries += 1
                self.session = self.restart_tor()

        error_message = 'Max retries exceeded with url {}.'.format(url)
        logging.error(error_message)
        raise TimeoutError(error_message)


    def get_urls(self, url_main='https://www.aruodas.lt/butu-nuoma/', url_listings='https://www.aruodas.lt/butu-nuoma/puslapis/', url_listings_settings='/?FOrder=AddDate&detailed_search=1'):
        '''
        TODO: Descr.
        # TODO: Move parameters to config file.

        :return:
        '''

        total_pages = self.get_number_of_pages(url_main)

        # Get listing urls for all of the pages. If an exception occurs while scraping, prints the exception.
        listing_urls = []
        for page_number in range(1, total_pages + 1):

            page_url = url_listings + str(page_number) + url_listings_settings

            page_urls = self.get_page_urls(page_url)
            listing_urls.extend(page_urls)


        return listing_urls


    def parse_object_data(self, soup, html_tags=None):
        '''
        soup - BS4 object.

        TODO: Create env variable for html tags or save them to a file somehow. Move to config file.
        TODO: Separate KEYVARIABLES from OPTIONAL VARIABLES. Wrap ALL optional variables with try except.
        '''

        if html_tags is None:
            pass

        # Initialize HTML class, id, etc. tags.
        html_tag_listing_statistics = 'obj-top-stats'

        html_tag_building_energy_class = 'energy-class-tooltip'

        html_tag_object_details = ['obj-details', 'obj-details ']  # Weird, but necessary.
        html_tag_object_details_names = 'dt'
        html_tag_object_details_items = 'dd'
        html_tag_object_description = 'collapsedText'
        html_tag_object_name = 'obj-header-text'

        html_tag_neighbourhood_statistics = 'advertStatisticHolder'
        html_tag_neighbourhood_statistics_names = 'cell-text'
        html_tag_neighbourhood_statistics_items = 'cell-data'

        html_tag_realtor = 'obj-contacts'
        html_tag_realtor_name = 'contacts-title'
        html_tag_no_realtor = 'obj-contacts simple'

        # Find all name and item classes withing object details class.
        object_details_class = soup.find(class_=html_tag_object_details[0])

        # Sometimes listings get displayed but have no info. Scraper catchers? TODO: Investigate.
        if object_details_class is None:
            return None

        object_names = object_details_class.find_all(html_tag_object_details_names)
        object_items = object_details_class.find_all(html_tag_object_details_items)

        # Get field names.
        object_names = [item.contents[0] for item in object_names]

        # Extract values from item fields.
        object_item_values = []
        for object_item in object_items:

            # Single value items.
            if len(object_item) == 1:
                object_item_values.append(object_item.contents[0])

            # Multiple value items.
            elif len(object_item) > 1:

                # Tries to get list-like items.
                items = [item.contents[0] for item in object_item.find_all(class_='special-comma')]
                if len(items) != 0:
                    object_item_values.append(items)

                # If the list is empty, the item is most likely single value (hence index 0), with additional ads, links, etc.
                else:
                    object_item_values.append(object_item.contents[0])

        # Set everything as a dictionary.
        object_data = dict(zip(object_names, object_item_values))

        # Find all name and item classes withing object details class.
        neighbourhood_statistics = soup.find(id=html_tag_neighbourhood_statistics)

        neighbourhood_statistics_names = neighbourhood_statistics.find_all(
            class_=html_tag_neighbourhood_statistics_names)
        neighbourhood_statistics_items = neighbourhood_statistics.find_all(
            class_=html_tag_neighbourhood_statistics_items)

        # Extract the name and the item values, add them to the dictionary.
        neighbourhood_statistics_names = [item.contents[0] for item in neighbourhood_statistics_names]
        neighbourhood_statistics_items = [item.contents[0] for item in neighbourhood_statistics_items]

        for name, item in zip(neighbourhood_statistics_names, neighbourhood_statistics_items):
            object_data[name] = item

        # Get object listing statistics (views, favorites, etc.).
        try:
            listing_statistics = soup.find(class_=html_tag_listing_statistics).contents
            object_data[listing_statistics[0]] = listing_statistics[1].contents[0]
        except:
            # TODO: This should ALWAYS have at least some values, but it breaks. Investigate
            object_data['Skelbimą peržiūrėjo (iš viso/šiandien):'] = '0/0'
            listing_statistics = None

        try:
            object_data['Listing Favorites'] = listing_statistics[3].contents[0]
        except:
            object_data['Listing Favorites'] = 0

        # Get object description text.
        object_data['Object Description'] = soup.find(id=html_tag_object_description).contents

        # Get object name - City, Neighbourhood, Street, Other.
        object_data['Listing Name'] = soup.find(class_=html_tag_object_name).contents[0]

        # Get energy class rating.
        building_energy_class = soup.find(class_=html_tag_building_energy_class)

        try:
            object_data['Building Energy Class'] = building_energy_class.contents[1]
            object_data['Building Energy Class Category'] = building_energy_class.contents[2]
        except:
            object_data['Building Energy Class'] = None
            object_data['Building Energy Class Category'] = None

        # Check if a realtor is present, get it's name if it is.
        object_contacts = soup.find(class_=html_tag_no_realtor)
        if object_contacts is None:
            realtor_name = soup.find(class_=html_tag_realtor).find(class_=html_tag_realtor_name).contents[0]
            # Handles cases where the realtor is a company, not a person. TODO: Add RealtorCompany variable.
            if realtor_name == 'Pardavėjo kontaktai':
                object_data['Realtor Name'] = None
                object_data['Realtor'] = False
            else:
                object_data['Realtor Name'] = realtor_name
                object_data['Realtor'] = True
        else:
            object_data['Realtor Name'] = None
            object_data['Realtor'] = False

        return object_data

    def get_object_data(self, listing_urls):
        '''
        TODO: Descr.
        :return:
        '''

        data = []

        # TODO: Delete this / find another way of logging.
        import tqdm

        for listing_url in tqdm.tqdm(listing_urls):
            # Move to config.
            driver = webdriver.Chrome('D:/Projects/Personal/Current/Rent AVM/rent-avm/scraper/chromedriver.exe')

            driver.get(listing_url)
            page_source = driver.page_source
            driver.close()

            soup = BeautifulSoup(page_source, 'lxml')

            # TODO: Add error handling based on page output.
            try:
                data.append(self.parse_object_data(soup))
            except:
                s = self.restart_tor()
                data.append(self.parse_object_data(soup))

        return data


    def process_object_data(self, object_data):
        '''
        TODO: Descr.
        '''
        variables_integer = ['Plotas', 'KainaMėn', 'KambariųSk', 'Aukštas', 'AukštųSk', 'Metai', 'ArtimiausiasDarželis',
                             'ArtimiausiaMokymoĮstaiga', 'ArtimiausiaParduotuvė', 'ViešojoTransportoStotelė',
                             'Nusikaltimai500MSpinduliuPraėjusįMėnesį']
        variables_categorical = ['PastatoTipas', 'Šildymas', 'Įrengimas']
        variables_lists = ['Ypatybės', 'PapildomosPatalpos', 'PapildomaĮranga', 'Apsauga']

        # Transform keys from whatever messy format to VariableName.
        keys = list(object_data.keys())
        keys = [key.strip() for key in keys]
        keys = [key.translate(str.maketrans('', '', string.punctuation)) for key in keys]
        keys = [''.join(key.lower().title().split(' ')) for key in keys]

        object_data = dict(zip(keys, list(object_data.values())))

        # Convert integer variables to integer.
        for variable in variables_integer:
            # Handles cases when variable is missing:
            try:
                object_data[variable] = int(re.findall('\d+', object_data[variable])[0])
            except:
                continue

        # Remove whitespace from categorical variables.
        for variable in variables_categorical:
            object_data[variable] = object_data[variable].strip()

        # Transform lists of variables into pseudo categorical variables. E.g. Variable: ['feat1', 'feat2'] -> Variable_feat1: 1, Variable_feat2: 1.
        for variable in variables_lists:
            try:
                # Convert space delimited text to TitleCamelCase. E.g. 'This house' -> 'ThisHouse'.
                object_data[variable] = [''.join(item.title().split(' ')) for item in object_data[variable]]

                # Create a pseudo categorical variable where multiple values in a category is present.
                for feature in object_data[variable]:
                    object_data[variable + '_' + feature] = 1
            except:
                continue

                # Split ListingName into City, Neighbourhood, Street.
        object_data['ListingName'] = object_data['ListingName'].split(',')
        object_data['BuildingCity'] = object_data['ListingName'][0].strip()
        object_data['BuildingNeighbourhood'] = object_data['ListingName'][1].strip()
        object_data['BuildingStreet'] = object_data['ListingName'][2].strip()

        # Split Total/Today views into separate variables.
        object_data['SkelbimąPeržiūrėjoIšVisošiandien'] = object_data['SkelbimąPeržiūrėjoIšVisošiandien'].split('/')
        object_data['ListingViewsTotal'] = int(object_data['SkelbimąPeržiūrėjoIšVisošiandien'][0])
        object_data['ListingViewsToday'] = int(object_data['SkelbimąPeržiūrėjoIšVisošiandien'][1])

        # Transform ObjectDescription into a single string, as well as replacing <br/>'s with \n.
        object_data['ObjectDescription'] = ''.join([str(item) for item in object_data['ObjectDescription']])
        object_data['ObjectDescription'] = re.sub('<br/s*?>', '\n', object_data['ObjectDescription'])

        # Drop no longer required variables.
        variables_drop = []
        variables_drop.extend(variables_lists)
        variables_drop.extend(['ListingName', 'SkelbimąPeržiūrėjoIšVisošiandien'])
        for variable in variables_drop:
            object_data.pop(variable)

        return object_data


    def scrape(self):
        '''
        TODO: Descr.

        :return:
        '''

        listing_urls = self.get_urls()
        data = self.get_object_data(listing_urls)
        # Transform list of dicts into a single DataFrame.
        df = pd.DataFrame(map(pd.Series, map(self.process_object_data, data)))


        return df
