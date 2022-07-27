"""
We run the script to identify invalid occupational codes. Those codes are taken away
for discussion and investigation. Eventually, we arrive at an agreed list of changes
to make to those invalid codes.

The process for updating the invalid codes happens in a different part of the code. This
step is just involved in identifying the invalid codes.
"""
import timeit
from data_connections import get_df_from_sql
from helpers import get_config
from pathlib import Path


def query_occ_codes_from_absence_data(database, staff_table_raw):
    """
    Function to extract OCCUPATION CODE from the raw absence data file to determine invalid codes to update

    Inputs:
        database: database name as defined in config file
        staff_table_raw: raw absence file name as defined in config file
        
    Output:
        Produces a dataframe containing every OCCUPATION CODE and TM YEAR MONTH from the raw file
    """
    query = f"""
        select
             [Tm Year Month]     as [TM_YEAR_MONTH]
            ,[OCCUPATION CODE]  as [OCCUPATION_CODE]
        from [{database}].[dbo].[{staff_table_raw}]
        group by [Tm Year Month], [Occupation Code]"""

    return get_df_from_sql(database, query)

def query_occ_codes_from_ref_data(database,
                                staff_table_raw,
                                ref_table,
                                org_master,
                                start_date,
                                end_date):
                                
    """
    Function to extract OCCUPAION CODES which will be included in the publication table based on link to ref data
    
    Inputs:
        database: database name as defined in config file
        staff_table_raw: raw absence file name as defined in config file
        ref_table: Occupation code ref table as defined in config file
        org_master: Organisation ref table as defined in config file
        start_date: Start date of data as defined in config file
        end_date: End date of data as defined in config file
        
    Output:
        A Dataframe containing OCCUPATION CODE and TM YEAR MONTH from the raw file which will be included in the publication
        
    """
    query = f"""
        select
             [Tm Year Month]     as [TM_YEAR_MONTH]
            ,[OCCUPATION CODE]  as [OCCUPATION_CODE]
        from [{database}].[dbo].[{staff_table_raw}] a
        left join [{database}].[dbo].[{ref_table}] b
            on a.[Occupation Code] = b.[occ_code]
        inner join [{database}].[dbo].[{org_master}] c
            on a.[ODS code] = c.[Current Org code]
        where (c.[End Date] >= '{end_date}' or c.[End Date] is null)
        and c.[Start Date] < '{start_date}'
        and c.[EnglandWales] = 'E'
        and c.[Reporting Org code] not in ('8HK67','8J318','X25','8J149','NL1')
        and c.[Reporting Org code] not like '[5Q]%'
        and (b.[END_DATE_PUBLICATION] >= '{end_date}' or b.[END_DATE_PUBLICATION] is null)
        and b.[START_DATE_PUBLICATION] < '{start_date}'
        group by [Tm Year Month], [Occupation Code], [MAIN_STAFF_GROUP_NAME], [STAFF_GROUP_1_NAME]
    """

    return get_df_from_sql(database, query)
  
def get_unexpected_occ_codes(absence_codes, ref_codes):
    """
    Function to look for cases where the values of OCCUPATION_CODE in the absence_codes table are not present in the ref_codes table
    
    Inputs:
        absence_codes: values from the current publication's table.
        ref_codes: valid values found in the reference code table.
        
    Output:
        unexpected_occ_codes dataframe containing a list of values of OCCUPATION CODE to investigate

    """
    unexpected_codes = absence_codes[
            ~absence_codes.set_index(["OCCUPATION_CODE"]).index.isin(
                ref_codes.set_index(["OCCUPATION_CODE"]).index)
            ]

    return unexpected_codes

def export_unexpected_occ_codes(unexpected_occ_codes, occ_codes_path):
    unexpected_occ_codes.to_csv(occ_codes_path, index=False)
    """
    Function to export unexpected_occ_codes csv containing a list of values of OCCUPATION CODE to investigate


    Inputs:
        unexpected_occ_codes: unexpected_occ_codes dataframe 
        occ_codes_path: output directory to put the unexpected_occ_codes_{start_date}.csv as 
                        specified in output_dir filepath in config file      
    Output:
        unexpected_occ_codes csv containing a list of values of OCCUPATION CODE to investigate

    """
    unexpected_occ_codes.to_csv(occ_codes_path, index=False)

def main():
    """
    Function to set up and run the code which exports the unexpected_occ_codes csv to the specified filepath
    """
    config = get_config()
    database = config['database']
    ref_table = config['ref_table']
    org_master = config['org_master']
    start_date = config['start_date']
    end_date = config['end_date']
    output_dir = Path(config['output_dir'])
    staff_table_raw = config['staff_table_raw']

    occ_codes_absence = query_occ_codes_from_absence_data(database, staff_table_raw)

    occ_codes_ref = query_occ_codes_from_ref_data(database,
                                                    staff_table_raw,
                                                    ref_table,
                                                    org_master,
                                                    start_date,
                                                    end_date)

    unexpected_occ_codes = get_unexpected_occ_codes(occ_codes_absence, occ_codes_ref)
    occ_codes_out_path = output_dir / f'unexpected_occ_codes_{start_date}.csv'
    export_unexpected_occ_codes(unexpected_occ_codes, occ_codes_out_path)

if __name__ == '__main__':
    print(f"Running checks to find bad occupation codes")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    print(f"Running time: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.")
