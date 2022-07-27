import pandas as pd
import logging

logger = logging.getLogger(__name__)

def sql_query_benchmark_data(database, staff_table, org_master, ref_table, start_date, end_date):
    """
    Creates a function to select the required fields for the benchmark tool data

    Inputs:
        database: database name as defined in config.toml file
        staff_table: base absence data table name as defined in the config.toml file
        org_master: Organisation ref table as defined in config.toml file
        ref_table: Occupation code ref table as defined in config.toml file
        start_date: Start date of data as defined in config.toml file
        end_date: End date of data as defined in config.toml file
            
    Output:
        SQL query which selects fields into a base set which is used to create the benchmark tool spreadsheet
    
    """
    logger.info("Preparing the base data SQL query")
    query = f"""
            select
                 [Wte Days Sick This Month] AS [FTE_DAYS_LOST]
                ,[Wte Days Available]       AS [FTE_DAYS_AVAILABLE]
                ,[NHSE_Region_Code]         AS [NHSE_REGION_CODE]
                ,[NHSE_Region_Name]         AS [NHSE_REGION_NAME]
                ,[Tm Year Month]            AS [TM_YEAR_MONTH]
                ,c.[STAFF_GROUP_1_NAME]
                ,b.[Reporting Org code]     AS [ORG_CODE]
                ,b.[Reporting Org name]     AS [ORG_NAME]
                ,b.[EnglandWales]           AS [ENGLAND_WALES]
                ,a.[Occupation Code]        AS [OCCUPATION_CODE]
            from [{database}].[dbo].[{staff_table}] a
            inner join [{database}].[dbo].[{org_master}] b
                on a.[ODS code] = b.[Current Org code]
            inner join [{ref_table}] c
                on a.[Occupation Code] = c.[occ_code]
            where
            (b.[End Date] >= '{end_date}' or b.[End Date] is null)
            and b.[Start Date] < '{start_date}'
            and b.[EnglandWales] = 'E'
            and b.[Reporting Org code] not in ('8HK67','8J318','8J149','NL1')
            and b.[Reporting Org code] not like '[5Q]%'
            and a.[Occupation Code] not like '[Z]%'
            and (c.[END_DATE_PUBLICATION] >= '{end_date}' or c.[END_DATE_PUBLICATION] is null)
            and c.[START_DATE_PUBLICATION] < '{start_date}'
        
        """
    return query

def sql_latest_org_name(database, latest_org_name):
    """
    Creates a function to select the most up to date organisation name from the REF_ORG_MASTER reference fields for the benchmark tool data

    Inputs:
        database: database name as defined in config.toml file
        latest_org_name: the REF_ORG_MASTER file to use as defined in config.toml file

    Output:
        SQL query which selects the most up to date organisation names/ and respective details to display
    """
    logger.info("Preparing the base data SQL query")
    query = f"""
            select
                 [NHSE_Region_Code]       as [NHSE_REGION_CODE]
                ,[NHSE_Region_Name]       as [NHSE_REGION_NAME]
                ,[ClusterGroup]           as [CLUSTER_GROUP]
                ,[BenchmarkGroup]         as [BENCHMARK_GROUP]
                ,[Reporting Org code]     as [ORG_CODE]
                ,[Reporting Org name]     as [ORG_NAME]
            from [{database}].[dbo].[{latest_org_name}]
            """
    return query

def all_staff_agg(df, cols_to_aggregate, cols_order):
    """ 
    Creates a function to produce a table of absence days available/ lost/ rates a total 'All staff groups' level for each organisation 
    
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        cols_order: Defined in the function create_benchmarking_tool as 'DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'ORG_NAME', 
        'CLUSTER_GROUP', 'BENCHMARK_GROUP','STAFF_GROUP', 'FTE_DAYS_LOST', 'FTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_PERCENT']
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates a total 'All staff groups' level for each organisation 
    
    """
    logger.info("Producing the all staff benchmarking orgs aggregation")

    df_agg = (df.groupby('ORG_CODE', as_index=False)
    .agg(cols_to_aggregate))

    df_agg['SICKNESS_ABSENCE_RATE_PERCENT'] = round(df_agg['FTE_DAYS_LOST'] / df_agg['FTE_DAYS_AVAILABLE'] * 100, 2)
    df_agg['NHSE_REGION_CODE'] = pd.NA
    df_agg['NHSE_REGION_NAME'] = pd.NA
    df_agg['STAFF_GROUP'] = 'All staff groups'
    df_agg = df_agg[cols_order]

    return df_agg

def medical_agg(df, cols_to_aggregate, cols_order):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates a total 'HCHS Doctors' level for each organisation    
        
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        cols_order: Defined in the function create_benchmarking_tool as 'DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'ORG_NAME', 
        'CLUSTER_GROUP', 'BENCHMARK_GROUP','STAFF_GROUP', 'FTE_DAYS_LOST', 'FTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_PERCENT']
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates a total 'HCHS Doctors' level for each organisation    
    
    """
    logger.info("Producing the medical staff benchmarking orgs aggregation")

    df_filtered = df[(df['STAFF_GROUP_1_NAME'].isin(['HCHS Doctors']))]

    df_agg = (df_filtered.groupby('ORG_CODE', as_index=False)
            .agg(cols_to_aggregate))

    df_agg['SICKNESS_ABSENCE_RATE_PERCENT'] = round(df_agg['FTE_DAYS_LOST'] / df_agg['FTE_DAYS_AVAILABLE'] * 100, 2)
    df_agg['NHSE_REGION_CODE'] = pd.NA
    df_agg['NHSE_REGION_NAME'] = pd.NA
    df_agg['STAFF_GROUP'] = 'HCHS Doctors'
    df_agg = df_agg[cols_order]

    return df_agg

def non_med_agg(df, cols_to_aggregate, cols_order):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates a non medical staff group level for each organisation    
    
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        cols_order: Defined in the function create_benchmarking_tool as 'DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'ORG_NAME', 
        'CLUSTER_GROUP', 'BENCHMARK_GROUP','STAFF_GROUP', 'FTE_DAYS_LOST', 'FTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_PERCENT']
    
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates a non medical staff group level for each organisation    
    
    """
    logger.info("Producing the non medical staff benchmarking orgs aggregation")

    df_filtered = df[df['OCCUPATION_CODE'].str.contains('[A-Z]')]

    df_agg = (df_filtered.groupby(['ORG_CODE', 'STAFF_GROUP_1_NAME'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'STAFF_GROUP_1_NAME': 'STAFF_GROUP'}, axis=1))

    df_agg['SICKNESS_ABSENCE_RATE_PERCENT'] = round(df_agg['FTE_DAYS_LOST'] / df_agg['FTE_DAYS_AVAILABLE'] * 100, 2)
    df_agg['NHSE_REGION_CODE'] = pd.NA
    df_agg['NHSE_REGION_NAME'] = pd.NA
    df_agg = df_agg[cols_order]

    return df_agg

def agg_benchmarking_orgs(df):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates a non medical staff group level for each organisation    
    
    Inputs:
        df: dataframe derived from the SQL Server query
       
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates a non medical staff group level for each organisation    
    
    """
    logger.info("Getting ready to calculate all the benchmarking breakdowns")
    cols_to_aggregate = {'FTE_DAYS_LOST': 'sum',
                        'FTE_DAYS_AVAILABLE': 'sum'}

    cols_order = ['ORG_CODE', 'STAFF_GROUP', 'FTE_DAYS_LOST', 'FTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_PERCENT']

    all_staff_groups = all_staff_agg(df, cols_to_aggregate, cols_order)
    medical_groups = medical_agg(df, cols_to_aggregate, cols_order)
    non_medical_groups = non_med_agg(df, cols_to_aggregate, cols_order)

    logger.info("Combining all of the benchmarking aggregations")
    df = pd.concat([all_staff_groups, medical_groups, non_medical_groups])
    df = df.replace({'Unknown': 'Other staff or those with unknown classification'})

    return df

def create_benchmarking_tool(df, df_query, month_date):
    """
    Creates a function that produces the final dataframe for the benchmarking tool csv output
    
    Inputs:
        df: dataframe derived from the SQL Server query
        df_query: a table containing the latest org names produced as a result of the sql_latest_org_name function
        month_date: month_date as defined in config.toml file
        
    Outputs: 
        Produces the final dataframe for the benchmarking tool csv output
    
    """
    logger.info("Getting ready to join to the latest org data")

    cols_order = ['DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'ORG_NAME', 
    'CLUSTER_GROUP', 'BENCHMARK_GROUP','STAFF_GROUP', 'FTE_DAYS_LOST', 'FTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_PERCENT']
    
    df['DATE'] = month_date
    # join the two dfs
    benchmarking_csv_outputs = df.merge(df_query, how='inner', on='ORG_CODE')
    benchmarking_csv_outputs = benchmarking_csv_outputs[cols_order]
    # suppress the data
    benchmarking_csv_outputs.loc[(benchmarking_csv_outputs["FTE_DAYS_AVAILABLE"] <= 330), "SICKNESS_ABSENCE_RATE_PERCENT"] = ''
    benchmarking_csv_outputs.loc[(benchmarking_csv_outputs["FTE_DAYS_AVAILABLE"] <= 330), "FTE_DAYS_LOST"] = ''
    benchmarking_csv_outputs.loc[(benchmarking_csv_outputs["FTE_DAYS_AVAILABLE"] <= 330), "FTE_DAYS_AVAILABLE"] = ''

    return benchmarking_csv_outputs
