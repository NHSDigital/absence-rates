Warning - this repository is a snapshot of a repository internal to NHS Digital. This means that some links may not work for external readers.

Repository owner: Workforce Analysis Team

Email: workforce.analysis@nhs.net

To contact us raise an issue on Github or via email and will respond promptly.


# Absence Rates

To set up the Absence Rates package enter the commands below (**run one line at a time**) in Anaconda Prompt (terminal on Mac/Linux):
```
conda env create --name absence --file environment.yml

conda activate absence
```
- _More on virtual environments: [Guide](https://github.com/NHSDigital/rap-community-of-practice/blob/main/python/virtual-environments.md)_

## Producing the publication
These are the steps to follow in order to produce the Absence Sickness rates publication:
1.	Update the excel data table templates
2.	Update dates to the current month of data in the config.toml file
3.	Run the data_quality_checks.py script ([details on what this does below](#data-quality-checks)).
4.	Update the NEW_VALUE column in the CSV file occupation_code_updates_yyyy_mm_dd 
5.	Run the make_publication.py script, details on what this code does are below
6.	Run the backtesting.


The process produces the following tables:
-	Monthly sickness absence tables
-	Monthly sickness absence CSV
-	Sickness absence by reason tables
-	Sickness absence by reason CSV
-	Sickness absence benchmarking tool CSV
-	COVID-19 related sickness absence CSV
-   unexpected_occ_codes (from data_quality_checks.py)

For new occupation codes in xxx:
-   occupation_code_updates

Outputs will be saved in the Outputs folder (xxx).

## High level walkthrough of the project structure
This section explains how the code runs from a narrative perspective. You can find out more by looking in each module and reading the docstrings that go along with each function.

The pipeline includes:
* [Loading the input parameters from the config.toml file](#import-the-config)
* [Preparing the base data](#prepare-base-data)
* [Read base data for aggregations](#read-base-data-for-aggregation)
* [Calculating breakdowns](#calculate-breakdowns)
* [Recording benchmarks and related descriptive outputs](#benchmarking-tool-and-covid-19-outputs)
* [Populating publication excels](#populate-excel)
* [Running the backtesting](#backtesting)

### Repository structure
```
absence-rates
├── .gitignore
├── README.md
├── environment.yml
├── setup.py
├── config.toml
├───absence_rates
│   ├── make_publication.py
│   ├── data_quality_checks.py
│   ├── benchmarking_tool.py
│   ├── data_connections.py
│   ├── covid_table.py
│   ├── preprocessing.py
│   ├── helpers.py
│   ├── reason_and_staff.py
│   ├── write_excel.py
│   └─── __init__.py
│    
├───excel_templates
│   ├───NHS_Sickness_by_reason_and_staff_template.xlsx
│   └───sickness_absence_monthl_template.xlsx
│
└───tests
    ├───backtesting
    │       │      backtesting_params.py
    │       │      __init__.py
    └───    └───   test_compare_outputs.py
```
- _More on Project structure (including setup.py and other standard repository files): [Guide](https://github.com/NHSDigital/rap-community-of-practice/blob/main/python/project-structure-and-packaging.md)_

### config.toml file
The config.toml file contains the publication parameters. Depending on the reporting month, tables, dates and any other parameters that require updating need to reflect the desired publication configuration. E.g. mds_table will change to the MDS table for the reporting publication month, month_date to the month of the publication etc.

### Data quality checks
The first step in the process is to run the `data_quality_checks.py` file.

This file should be run when new data arrives. It looks at the `_RAW` data (SQL Server table) and finds any invalid occupation codes. If it finds invalid codes it exports them to a CSV file, in the Outputs folder (xxx). You can use this CSV file to decide how you want to update those invalid codes.

Once you have decided how those invalid codes should be handled follow the steps below:
1.	Create a copy of the template input file occupation_code_updates_yyyy-mm-dd.csv(located in the Inputs folder (xxx) 
2.	Replace the yyyy_mm_dd with the publication month you are running data for.   
3.	Add the invalid codes from OCCUPATION_CODE column in the unexpected_occ_codes_yyyy_mm_dd file into the OLD_VALUE column in occupation_code_updates_yyyy-mm-dd.csv. 
4.	Add the respective valid occupation code for each invalid code to the NEW_VALUE column.
The rest of the code will then read in this file and complete the rest of the publication using the valid codes.

## Making the publication.
The rest of the SQL process can be run in a single go by using the `make_publication.py` code. This top-level script runs each of the sub-processes one at a time.

You can run this file by calling in Anaconda Prompt (terminal on Mac/Linux):
~~~
python .\absence_rates\make_publication.py
~~~


Listed below are the sub-processes in the make_publication.py script alongside a brief explanation:

#### Import the config
When you run this code the first function to call is `get_config()`. This looks in our folder directory for the config.toml file and stores in this information into a dictionary. We are then going to take out `database`, `staff_table`, `ref_table`, etc, from the config dictionary and store them in variables (with the same name) to use later. Ensure that the parameters related to the current publication are changed e.g. `month_date` etc.

#### Prepare base data
The next function to run is `prepare_base_data()` - located in the `preprocessing.py` file. It copies all the data from the raw table, e.g., 'ESR-ABSENCE-yyyy-mm_RAW' and uses it to populate the processed table: 'ESR-ABSENCE-yyyy-mm_PROCESSED' whilst also updating invalid occupation codes identified in the data_quality_checks.py.

#### Read base data for aggregation
The next step is to read the base data into Python in a format that makes it easy for us to do aggregations. This ensures we have all the columns that we need, and all of the appropriate filters have been applied. By doing this step here, we avoid repetitive code later in the process.

We use two functions to do this: `query_base_data()` and `get_df_from_sql()` - located in `absence_rates.py` and `data_connections.py`. The first function constructs a SQL query for us, passing in the information from the config file. The second function runs that query and returns the Python dataframe.

#### Calculate breakdowns
The next step in the process is to calculate all of the breakdowns. We have split this into two main functions: `create_absence_rates_breakdowns()` and `create_org_absence_breakdowns()` - both located in `absence_rates.py`.

The `create_absence_rates_breakdowns()` function calculates these breakdowns:
- England
- Regional
- Major staff groups (aka 'MAIN_STAFF_GROUP_NAME')
- Minor staff groups (aka 'STAFF_GROUP_1_NAME')
- Minor staff grades (aka 'GRADE')
- Cluster groups (aka 'CLUSTER')

You can see each of these breakdowns being calculated in the function:
```python
def create_absence_rates_breakdowns(df):
    cols_to_aggregate = {'WTE_DAYS_SICK_THIS_MONTH': 'sum',
                        'WTE_DAYS_AVAILABLE': 'sum'}

    cols_order = ['BREAKDOWN_TYPE', 'BREAKDOWN_VALUE', 'NHSE_REGION_CODE',
                'NHSE_REGION_NAME', 'WTE_DAYS_SICK_THIS_MONTH', 'WTE_DAYS_AVAILABLE', 'ABSENCE_RATE']

    all_england = agg_all_england(df, cols_to_aggregate, cols_order)
    regional = agg_regions(df, cols_to_aggregate, cols_order)
    major_staff_groups = agg_major_staff_groups(df, cols_to_aggregate, cols_order)
    minor_staff_groups = agg_minor_staff_groups(df, cols_to_aggregate, cols_order)
    minor_staff_grades = agg_minor_staff_grades(df, cols_to_aggregate, cols_order)
    cluster_groups = agg_cluster_groups(df, cols_to_aggregate, cols_order)

    csv_1_outputs = pd.concat([all_england, regional, major_staff_groups,
                                minor_staff_groups, minor_staff_grades,
                                cluster_groups])

    return csv_1_outputs
```

By contrast, the `create_org_absence_breakdowns()` function only calculates stats for the reporting orgs. We kept this as a separate step because the reporting orgs data is so long. All of the other breakdowns fit neatly into one CSV.

#### Populate excel
The final step in the `make_publication()` function is to populate the excel tables. To do this we need two inputs:
- The data from the CSVs
- The excel template

The functions `prepare_table_1()`, `prepare_table_2()` and `prepare_table_3()`(located in `write_excel.py`) read in the CSV data and do some wrangling to get the data in the right shape for the excel table. Then, the `write_tables_to_excel()` function (located in `write_excel.py`) goes through and pastes the data into each sheet. For table 4 of absence rates data tables Excel file, use the xlookup formula on CSV-2 Absence rates `=XLOOKUP(D5,'main_csv_2_yyyy-mm-dd.csv'!$D$2:$D$336,'main_csv_2_yyyy-mm-dd.csv'!$I$2:$I$336, ".")` and paste to all rows, and the Regional average rates from Table 1.

#### Benchmarking tool and COVID-19 outputs
Running the `make_publication.py` script will run the entire publication pipeline, including the `benchmarking_tool.py` and `covid_table.py` scripts. 
- Input parameters are read from `config.toml`, ensure if you are running the publication for a specific month, that the correct equivalent reference tables are in the configuration file. 
- Outputs are stored in the Outputs folder in the ic.green Workforce RAP directory. The output format is `benchmarking_csv_{start_date}.csv` and `covid_{start_date}.csv`.
- Suppression is applied to the data where the FTE days available value is 330 or less in line with The Data Protection Act.

#### Reason and staff output 
Running the `make_publication.py` script will also run the `reason_and_staff.py` script. 
- Input parameters are read from `config.toml`, ensure if you are running the publication for a specific month, that the correct equivalent reference tables are in the configuration file. 
- Outputs are stored in the Outputs folder in the ic.green Workforce RAP directory. The output format is `reason_absence_{start_date}.csv`
- Within the `reason_and_staff.py` script there is a list of accepted absence reasons and ignored staff groups. Please update these if there are any changes. 

#### Backtesting 
Before running the `test_compare_outputs` script, ensure the current publication's outputs produced from the SQL pipeline are in the ground truth folder located in xxx and the outputs produced from the RAP pipeline in the Outputs_to_test folder. In `backtesting_params` ensure that the correct CSVs are selected for each folder, then run `test_compare_outputs`.

For more info on backtesting [click](https://github.com/NHSDigital/rap-community-of-practice/blob/main/development-approach/10_backtesting.md).

## Licence
Absence rates codebase is released under the MIT License.

The documentation is © Crown copyright and available under the terms of the [Open Government 3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) licence.
