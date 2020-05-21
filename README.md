# rent-avm

This is a project description for an automated rent price automated valuation model (Rent-AVM).
<br><br/>

## General project structure

Project pipeline can be split into three main parts:
  * Scraper
  * Machine Learning Model
  * API and Website
  
<br><br/>
  
## Poject Pipeline
### Scraper
Scraper should run once a day - get the data, update existing data, verify formats and report any changes.

In addition to this, any static datasets should be combined to existing data at the end of scraper process. These datasets are updated less frequently, therefore a manual updation process is feasible. Otherwise, it could also be automated.
<br><br/>

### Machine Learning Model
Machine learning model should also be retrained daily, prioritizing the most recent data to stay updated as well as adjust based on new inputs.
<br><br/>

### API and Website
A trained ML Model should be added to an API, which is connected to a website for the end user.
<br><br/>

## Progress:

[] Scraper:
 * [x] Base Scraper
 * [] Format Verifier
 * [] Static Datasets

[] Machine Learning Model:
 * [] Preprocessing
 * [] Training
 * [] Validation and Metric Monitoring
  
[] API and Website:
 * [] Model API
 * [] Website
  
  
