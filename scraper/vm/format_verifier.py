import numpy as np
import pandas as pd

import json

from scipy import stats


class NpEncoder(json.JSONEncoder):
    """
    Class "stolen" from stack overflow to handle Lithuanian encodings for the json library.
    """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


class FormatVerifier:
    def __init__(self, logger, p_value=0.05, missing_value_deviation=0.1):
        """
        Class used to verify the various formats of the dataset acquired from the scraper.

        Parameters
        ----------
        p_value (float): p-value limit which, if exceeded, is considered to have failed the statistical test.
        missing_value_deviation (float): maximum allowed percentage deviation from the historical values until which
         it is considered accepted.
        """

        self.p_value = p_value
        self.missing_deviation = missing_value_deviation
        self.logger = logger

        # Load config file.
        with open('config/config_verifier.json', 'r', encoding='utf-8') as f:
            self.config = json.load(f)

        # Load, if data_info tables exist.
        from pandas_gbq.gbq import GenericGBQException

        try:
            # Load the data from GoogleBigQuery and store into a dict.
            variable_names = list(
                pd.read_gbq("select * from data_info.variable_names", project_id='rent-avm')['VariableNames'].values)
            value_names = list(
                pd.read_gbq("select * from data_info.value_names", project_id='rent-avm')['ValueNames'].values)
            statistics = \
                pd.read_gbq("select * from data_info.statistics", project_id='rent-avm').set_index('Name').T.to_dict()

            self.historical_info = {
                'names': {
                    'variable_names': variable_names,
                    'value_names': value_names
                },
                'statistics': statistics
            }

        except GenericGBQException:
            self.historical_info = {
                'names': {
                    'variable_names': [],
                    'value_names': []
                },
                'statistics': {}
            }

        pass

    def t_test(self, x_stats, y_stats):
        """
        A customized t-test.

        Sources:
            [1] https://www.medcalc.org/calc/comparison_of_means.php
            [2] https://towardsdatascience.com/inferential-statistics-series-t-test-using-numpy-2718f8f9bf2f

        Parameters
        ----------
        x_stats (dict): dictionary containing the relevant information: number of samples, standard deviation, mean.

        y_stats (dict): dictionary containing the relevant information: number of samples, standard deviation, mean.

        Returns
        -------
        p-value of the test statistic.
        """
        # Get the pooled standard deviation.
        s = np.sqrt(
            ((x_stats['samples'] - 1) * x_stats['std'] ** 2 + (y_stats['samples'] - 1) * y_stats['std'] ** 2) / (
                        x_stats['samples'] + y_stats['samples'] - 2)
        )

        # Get the t-statistic. + 0.0001 is to avoid dividing by 0.
        t = (x_stats['mean'] - y_stats['mean']) / (s * np.sqrt(2 / (x_stats['samples'] + y_stats['samples'])) + 0.0001)

        # Get the degrees-of-freedom.
        df = 2 * (x_stats['samples'] + y_stats['samples']) - 2

        # Get the p-value.
        p = 1 - stats.t.cdf(t, df=df) if not np.isnan(t) else np.nan
        return p

    def check_names(self, df):
        """
        Checks for any new Variables and Values by comparing them to historical_info file.

        Some pseudo-categorical (in a sense of possibility of having one than one category in the variable) Variables
        are joined with their Values with an underscore "_" , i.e. Variable_Value, thus these columns are split to
        check for any new additions in the data.

        Parameters
        ----------
        df (pandas.DataFrame): consists of data to be checked.
        """

        column_names = df.columns.values

        # Split pseudo-categorical variables to get their Variable and Value information.
        column_names_split = [name.split('_') for name in column_names]
        variable_names = [name[0] for name in column_names_split if len(name) == 1]
        value_names = [name[1] for name in column_names_split if len(name) == 2]

        # Check and report if all of the old variables are present.
        # Values are not checked because they can vary day-to-day.
        variable_names_not_found = [name for name in self.historical_info['names']['variable_names'] if
                                    name not in variable_names]

        if len(variable_names_not_found) > 0:
            self.logger.warning('Variables expected, but not found in the dataset: {}'.format(variable_names_not_found))
        else:
            self.logger.info('Found all of the expected Variables.')

        variable_names_new = [name for name in variable_names if
                              name not in self.historical_info['names']['variable_names']]
        value_names_new = [name for name in value_names if name not in self.historical_info['names']['value_names']]

        # Report any new Variable/Value names if any were found.
        if len(variable_names_new) > 0:
            self.logger.warning('Found previously unseen Variables: {}'.format(variable_names_new))
            self.historical_info['names']['variable_names'].extend(variable_names_new)
        else:
            self.logger.info('Found no new Variables')

        if len(value_names_new) > 0:
            self.logger.warning('Found previously unseen value names: {}'.format(value_names_new))
            self.historical_info['names']['value_names'].extend(value_names_new)
        else:
            self.logger.info('Found no new Values.')

        pass

    def check_types(self, df):
        """
        Checks if there are any new "object" type variables that were not present in the config file.

        Parameters
        ----------
        df (pandas.DataFrame): consists of data to be checked.
        """

        names_strings = df.select_dtypes('object').columns.values
        names_strings_unexpected = [name for name in names_strings if name not in self.config['types']['string']]

        if len(names_strings_unexpected) > 0:
            self.logger.warning(
                'Found variables are not expected to be "object" type: {}.'.format(names_strings_unexpected))
        else:
            self.logger.info('Found no new "ojbect" type variables.')
        pass

    def check_statistics(self, df):
        """
        Performs a t-test for variable means where historical information exists as well as checks if the missing
        value percentage is less than the configuration deviation.

        p-value limit and missing value percentage deviation limit can be set in the initialization of this class.

        Updates the existing historical statistics where possible and creates statistics for new variables.

        Parameters
        ----------
        df (pandas.DataFrame): consists of data to be checked.
        """

        # Get current batch statistics, add missing value percentage as well as number of samples.
        df_numeric = df.select_dtypes(exclude='object')
        if df_numeric.empty:
            statistics = pd.DataFrame()
        else:
            statistics = df_numeric.describe().T
            statistics = statistics[['mean', 'std', 'min', 'max']]

        statistics['missing'] = df.isna().mean()
        statistics['samples'] = df.count()
        statistics['samples_total'] = df.shape[0]
        statistics['sum'] = df.sum()
        statistics['sum_squares'] = (df_numeric ** 2).sum()

        # Split variables into ones with historical statistical data and ones without.
        variables_existing = [name for name in statistics.index.values if
                              name in self.historical_info['statistics'].keys()]
        variables_new = [name for name in statistics.index.values if
                         name not in self.historical_info['statistics'].keys()]

        # Save new variable statistics and report.
        for variable in variables_new:
            self.historical_info['statistics'][variable] = statistics.loc[variable].to_dict()

        self.logger.info('Saved new statistics for Variables: {}'.format(variables_new))

        # Statistical tests.
        variables_failed_test = []
        variables_failed_missing = []
        for variable in variables_existing:

            # Perform a t-test, add to variables_failed if it failed the test.
            p_value = self.t_test(self.historical_info['statistics'][variable], statistics.loc[variable].to_dict())
            if p_value <= self.p_value:
                variables_failed_test.append(variable)

            # Compare missing values with self.missing_deviation to see if it's more than expected.
            missing_difference = abs(
                self.historical_info['statistics'][variable]['missing'] - statistics.loc[variable, 'missing'])
            if missing_difference >= self.missing_deviation:
                variables_failed_missing.append(variable)

            # Update the historical info of existing variables.
            # Mean, sample sizes.
            samples_total_new = self.historical_info['statistics'][variable]['samples_total'] + statistics.loc[
                variable, 'samples_total']
            samples_new = self.historical_info['statistics'][variable]['samples'] + statistics.loc[variable, 'samples']
            mean_new = (self.historical_info['statistics'][variable]['mean'] + statistics.loc[
                variable, 'mean']) / samples_new

            # Update the standard deviation. Source: https://stackoverflow.com/questions/1174984/how-to-efficiently-calculate-a-running-standard-deviation
            sum_total = statistics.loc[variable, 'sum'] + self.historical_info['statistics'][variable]['sum']
            sum_total_squares = statistics.loc[variable, 'sum_squares'] + self.historical_info['statistics'][variable][
                'sum_squares']
            n_samples = statistics.loc[variable, 'samples'] + statistics.loc[variable, 'samples']

            # Get standard error if sigma is > 0, otherwise set it to nan.
            sigma = (sum_total_squares / n_samples) - (sum_total / n_samples) ** 2
            std_new = np.sqrt(sigma) if sigma >= 0 else np.nan

            # Update min and max values.
            if statistics.loc[variable, 'min'] <= self.historical_info['statistics'][variable]['min']:
                min_new = statistics.loc[variable, 'min']
            else:
                min_new = self.historical_info['statistics'][variable]['min']

            if statistics.loc[variable, 'max'] >= self.historical_info['statistics'][variable]['max']:
                max_new = statistics.loc[variable, 'max']
            else:
                max_new = self.historical_info['statistics'][variable]['max']

            # Update the missing value percentage.
            missing_weight_old = self.historical_info['statistics'][variable]['samples_total'] / (
                        self.historical_info['statistics'][variable]['samples_total'] + statistics.loc[
                    variable, 'samples_total'])
            missing_weight_new = statistics.loc[variable, 'samples_total'] / (
                        self.historical_info['statistics'][variable]['samples_total'] + statistics.loc[
                    variable, 'samples_total'])

            missing_new = self.historical_info['statistics'][variable]['missing'] * missing_weight_old + statistics.loc[
                variable, 'missing'] * missing_weight_new

            # Set the updated values to historical_info.
            self.historical_info['statistics'][variable] = {
                'std': std_new,
                'mean': mean_new,
                'min': min_new,
                'max': max_new,
                'missing': missing_new,
                'samples': samples_new,
                'samples_total': samples_total_new,
                'sum': sum_total,
                'sum_squares': sum_total_squares
            }

        # Log the results.
        self.logger.info('Updated statistics for all existing Variables.')

        if len(variables_failed_test) > 0:
            self.logger.warning(
                'Found Variables that have failed the statistical test with p-value of {0}: {1}.'.format(
                    self.p_value, variables_failed_test))
        else:
            self.logger.info(
                'All variables passed the statistical tests succesfully with a p-value of {0}.'.format(self.p_value))

        if len(variables_failed_missing) > 0:
            self.logger.warning(
                'Found Variables that have failed the missing value check with missing value percentage deviation of {0}: {1}.'.format(
                    self.missing_deviation, variables_failed_missing))
        else:
            self.logger.info(
                'All variables passed the missing value check with missing value percentage deviation of {0}.'.format(
                    self.missing_deviation))

        # Split variables into representative tables and upload to GoogleBigQuery.
        gbq_variable_names = pd.DataFrame(self.historical_info['names']['variable_names'], columns=['VariableNames'])
        gbq_value_names = pd.DataFrame(self.historical_info['names']['value_names'], columns=['ValueNames'])
        gbq_statistics = pd.DataFrame(self.historical_info['statistics']).T.reset_index().rename(columns={'index': 'Name'})

        gbq_variable_names.to_gbq('data_info.variable_names', project_id='rent-avm', if_exists='replace', progress_bar=False)
        gbq_value_names.to_gbq('data_info.value_names', project_id='rent-avm', if_exists='replace', progress_bar=False)
        gbq_statistics.to_gbq('data_info.statistics', project_id='rent-avm', if_exists='replace', progress_bar=False)

        self.logger.info('Succesfully updated historical info and uploaded to GoogleBigQuery.')
        pass

    def verify(self, df):
        """
        Performs all the verification checks in the class for a given dataset.

        Currently, these are:
         * check_names()
         * check_types()
         * check_statistics()

        Parameters
        ----------
        df (pandas.DataFrame): consists of data to be checked.
        """

        self.logger.info('Executing data checks.')
        self.check_names(df)
        self.check_types(df)
        self.check_statistics(df)
        self.logger.info('Successfully executed all the data checks.')

        pass
