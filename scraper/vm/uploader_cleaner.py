# TODO: Delete Lithuanian columns that were added to the master data before the rename in the load method.

import pandas as pd
from pandas_gbq.gbq import InvalidSchema, GenericGBQException
from google.cloud import bigquery

import json
import re
import datetime


class UploaderCleaner:
    def __init__(self, logger):
        """
        Class that routinely uploads and cleans up Google Big Query to save space usage and costs.

        Parameters
        ----------
        logger : logging object to be used.
        """

        # Initialize class variables.
        self.logger = logger

        # Initialize class variables that will be set later.
        self.df_most_recent = None
        self.table_ids_most_recent = None
        self.df_current = None

        # Load the config files.
        # TODO: Move this somewhere else in order to be able to edit without having to recreate the docker image each
        #  time. Where?
        with open("config/config_uploader_cleaner.json") as f:
            self.config = json.load(f)
        pass

    def extract(self):
        """
        Extracts the most recent datasets with the format of "config_file_input + _YYYY_MM_DD" from Google Big Query.
        Stores it in self.df_most_recent.

        Returns
        -------
        self
        """

        self.logger.info('Extracting data from Google Big Query.')
        name_dataset = self.config['table_names']['name_dataset']
        name_table_raw_listings = self.config['table_names']['name_table_raw_listings']

        # Concat all the most recent tables in dataset_name into a single dataframe. Should always be 1 table only.
        client = bigquery.Client()
        table_ids = [table.table_id for table in client.list_tables(name_dataset)]

        table_ids_most_recent = [table_id for table_id in table_ids
                              if re.match(name_table_raw_listings + '_\d{4}_\d{2}_\d{2}', table_id)]

        # Sets an empty dataframe if no tables were found.
        if len(table_ids_most_recent) == 0:
            self.df_most_recent = pd.DataFrame()
        else:
            self.df_most_recent = pd.concat(
                [pd.read_gbq(f'select * from {name_dataset}.{table_id}') for table_id in table_ids_most_recent]
            )
            self.table_ids_most_recent = table_ids_most_recent  # These will get deleted at the 'load' part.

        self.logger.info('Found these most recent tables: {}'.format(table_ids_most_recent))
        return self

    def transform(self, data, return_transformed_data=False):
        """
        Transforms the data. Currenly performed transformations:
         * Renames the columns based on the config file;
         * Concatenates with the previously found daily datasets;
         * Removes entries older than 7 days.

        Planned transformations:
         * Get and Join static datasets to this daily data.

        Parameters
        ----------
        data (pandas.dataframe) : Data to be transformed.
        return_transformed_data (bool) : Whether to return the transformed dataset. If False, stores it in
                                         self.df_current.

        Returns
        -------
        self, pandas.dataframe
        """

        # def etl_static():
        #     """Extracts, transforms and loads the static datasets from GBQ."""
        #     # TODO: Get static datasets.
        #     return None

        # def join_static(df):
        #     """Joins input dataframe with static dataframes and returns the complete dataframe."""
        #     # TODO: Make this.
        #     df_static = etl_static()
        #     df = pd.concat([df, df_static])
        #     return df

        self.logger.info('Processing the input data and joining with most recently available datasets.')
        # Process the input data.
        df_raw = data.copy()
        df_new = df_raw.rename(columns=self.config['column_names'])

        # Join with the most recent dataset, remove duplicates based on date.
        df_current = pd.concat([self.df_most_recent, df_new])

        # If the information is the same, keep only the most recently scraped data.
        df_current = df_current.sort_values('DateScraped', ascending=False)
        df_current = df_current.drop_duplicates(subset=df_current.columns.difference(['DateScraped']), keep='first')

        # Delete entries older than 7 days from the daily data to reduce it's size.
        # Identical entries within the master data with scraping time difference of 7 days are considered different.
        # Why? Inflation bruh, fight me.
        df_current['DateScraped'] = pd.to_datetime(df_current['DateScraped'])
        df_current['DateDiffDays'] = (
                    pd.to_datetime(datetime.datetime.today().date()) - df_current['DateScraped']).dt.days

        df_current = df_current.loc[df_current['DateDiffDays'] <= 7]
        df_current = df_current.drop('DateDiffDays', axis=1)

        self.logger.info('Successfully transformed the data.')

        if return_transformed_data:
            return df_current
        else:
            self.df_current = df_current
            return self

    def load(self, data=None):
        """
        Adds data to the master dataset, uploads current daily data and deletes the previously, now outdated, daily
        datasets in Google Big Query.

        Parameters
        ----------
        data (pandas.dataframe) : (Optional) Data to be uploaded. If None, uses self.df_current from
                                  .extract().transform()

        Returns
        -------
        response (json) : Confirmation message of completion.
        """

        # Allow for separate usage from other methods.
        if data is None:
            df_current = self.df_current
        else:
            df_current = data.copy()

        name_dataset = self.config['table_names']['name_dataset']
        name_table_raw_listings_master = self.config['table_names']['name_table_raw_listings_master']
        name_table_raw_listings = self.config['table_names']['name_table_raw_listings']

        # Tries to append to existing master dataset.
        try:
            df_current.to_gbq(f'{name_dataset}.{name_table_raw_listings_master}',
                              project_id='rent-avm', if_exists='append', progress_bar=False)
            self.logger.info('Appended current dataset to existing master dataset.')
        # Uploads new dataset if no master dataset present.
        except GenericGBQException:
            self.logger.warning('No master dataset was found, uploading current data as master dataset.')
            df_current.to_gbq(f'{name_dataset}.{name_table_raw_listings_master}',
                              project_id='rent-avm', if_exists='replace', progress_bar=False)
        # Adds new columns by concatenating and re-uploading.
        except InvalidSchema:
            self.logger.warning('New columns were found, downloading and re-uploading the master dataset.')
            df_master = pd.read_gbq(
                f'select * from  {name_dataset}.{name_table_raw_listings_master}',
                project_id='rent-avm'
            )

            df_master = pd.concat([df_master, df_current])
            df_master.to_gbq(f'{name_dataset}.{name_table_raw_listings_master}',
                             project_id='rent-avm', if_exists='replace', progress_bar=False)

        # Uploads current daily data and deletes the previously found one, if any.
        self.logger.info('Uploading current daily data.')
        name_table_current =\
            f"{name_dataset}.{name_table_raw_listings}_{datetime.datetime.today().date().strftime('%Y_%m_%d')}"
        df_current.to_gbq(name_table_current, project_id='rent-avm', if_exists='replace', progress_bar=False)

        if not self.df_most_recent.empty:
            client = bigquery.Client()
            self.logger.info('Deleting older daily datasets: {}.'.format(self.table_ids_most_recent))
            for table_id in self.table_ids_most_recent:
                client.delete_table(f'{name_dataset}.{table_id}')

        self.logger.info('Successfully cleaned up the Google Big Query.')
        return json.dumps({'message': 'Successfully cleaned up the Google Big Query.'})

    def etl(self, data):
        """Combines all the main methods in the class into a single ETL method - .extract(), .transform(), .load()."""
        return self.extract().transform(data).load()
