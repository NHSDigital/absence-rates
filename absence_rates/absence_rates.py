import pandas as pd
import logging

logger = logging.getLogger(__name__)

def query_base_data(database, staff_table, org_master, ref_payscale, ref_table, start_date, end_date):
    """
    Creates a function to select the required fields for the base absence data

    Inputs:
        database: database name as defined in config.toml.toml file
        staff_table: base absence data table name as defined in the config.toml file
        org_master: Organisation ref table as defined in config.toml file
        ref_payscale: Payscale code ref table as defined in config.toml file
        ref_table: Occupation code ref table as defined in config.toml file
        start_date: Start date of data as defined in config.toml file
        end_date: End date of data as defined in config.toml file
        
    Output:
        SQL query which selects a base set of fields to be used for the creation of the Sickness Absence publication
    """
    logger.info("Preparing the base data SQL query")
    query = f"""
            select
                 [Wte Days Sick This Month] as [FTE_DAYS_LOST]
                ,[Wte Days Available]       as [FTE_DAYS_AVAILABLE]
                ,[NHSE_Region_Code]         as [NHSE_REGION_CODE]
                ,[NHSE_Region_Name]         as [NHSE_REGION_NAME]
                ,[Tm Year Month]            as [TM_YEAR_MONTH]
                ,b.[MAIN_STAFF_GROUP_NAME]
                ,b.[STAFF_GROUP_1_NAME]
                ,c.[ClusterGroup]           as [CLUSTER_GROUP]
                ,c.[Reporting Org code]     as [ORG_CODE]
                ,c.[Reporting Org name]     as [ORG_NAME]
                ,c.[EnglandWales]           as [ENGLAND_WALES]
                ,d.[GRADE]
            from [{database}].[dbo].[{staff_table}] a
            left join [{ref_table}] b
                on a.[Occupation Code] = b.[occ_code]
            inner join [{database}].[dbo].[{org_master}] c
                on a.[ODS code] = c.[Current Org code]
            left join [{ref_payscale}] d
                on a.[Grade Code] = d.[PAYSCALE_CODE]
            where
            (c.[End Date] >= '{end_date}' or c.[End Date] is null)
            and c.[Start Date] < '{start_date}'
            and c.[EnglandWales] = 'E'
            and c.[Reporting Org code] not in ('8HK67','8J318','8J149','NL1')
            and c.[Reporting Org code] not like '[5Q]%'
            and (b.[END_DATE_PUBLICATION] >= '{end_date}' or b.[END_DATE_PUBLICATION] is null)
            and b.[START_DATE_PUBLICATION] < '{start_date}'
            """
    return query

def agg_all_england(df, cols_to_aggregate, cols_order):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates at a total England level 
        
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        cols_order: Defined in the function agg_reporting_orgs as 'BREAKDOWN_TYPE', 'BREAKDOWN_VALUE', 
        'NHSE_REGION_CODE', 'NHSE_REGION_NAME',  'REPORTING_ORG_CODE', 'REPORTING_ORG_NAME',
        'WTE_DAYS_SICK_THIS_MONTH', 'WTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_RATE'
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates at a total England level 
    """
    logger.info("Producing the all England aggregation")

    df_agg = (df.groupby('ENGLAND_WALES', as_index=False)
            .agg(cols_to_aggregate)
            .rename({'ENGLAND_WALES': 'BREAKDOWN_VALUE_1'}, axis=1))

    df_agg['SICKNESS_ABSENCE_RATE_PERCENT'] = round(df_agg['FTE_DAYS_LOST'] / df_agg['FTE_DAYS_AVAILABLE'] * 100, 2)
    df_agg['BREAKDOWN_VALUE_2'] = pd.NA
    df_agg['NHSE_REGION_CODE'] = pd.NA
    df_agg['NHSE_REGION_NAME'] = pd.NA
    df_agg['BREAKDOWN_TYPE'] = 'ALL_ENGLAND'
    df_agg['BREAKDOWN_VALUE'] = 'ALL_ENGLAND'
    df_agg = df_agg[cols_order]
    return df_agg

def agg_regions(df, cols_to_aggregate, cols_order):
    """ 
    Creates a function to produce a table of absence days available/ lost/ rates at a total Region level 
        
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        cols_order: Defined in the function agg_reporting_orgs as 'BREAKDOWN_TYPE', 'BREAKDOWN_VALUE', 
        'NHSE_REGION_CODE', 'NHSE_REGION_NAME',  'REPORTING_ORG_CODE', 'REPORTING_ORG_NAME',
        'WTE_DAYS_SICK_THIS_MONTH', 'WTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_RATE'
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates at a total Region level 
    """
    logger.info("Producing the regional aggregation")

    df_agg = (df.groupby(['NHSE_REGION_CODE', 'NHSE_REGION_NAME'], as_index=False)
            .agg(cols_to_aggregate))

    df_agg['SICKNESS_ABSENCE_RATE_PERCENT'] = round(df_agg['FTE_DAYS_LOST'] / df_agg['FTE_DAYS_AVAILABLE'] * 100, 2)
    df_agg['BREAKDOWN_VALUE'] = df_agg['NHSE_REGION_NAME']
    df_agg['BREAKDOWN_TYPE'] = 'REGION'
    df_agg = df_agg[cols_order]
    return df_agg

def agg_major_staff_groups(df, cols_to_aggregate, cols_order):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates by main staff group 
        
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        cols_order: Defined in the function agg_reporting_orgs as 'BREAKDOWN_TYPE', 'BREAKDOWN_VALUE', 
        'NHSE_REGION_CODE', 'NHSE_REGION_NAME',  'REPORTING_ORG_CODE', 'REPORTING_ORG_NAME',
        'WTE_DAYS_SICK_THIS_MONTH', 'WTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_RATE'

    Outputs:
        Produces a dataframe absence days available/ lost/ rates by main staff group 
    """
    logger.info("Producing the major staff group aggregation")

    df_agg = (df.groupby('MAIN_STAFF_GROUP_NAME', as_index=False)
            .agg(cols_to_aggregate)
            .rename({'MAIN_STAFF_GROUP_NAME': 'BREAKDOWN_VALUE'}, axis=1))

    df_agg['SICKNESS_ABSENCE_RATE_PERCENT'] = round(df_agg['FTE_DAYS_LOST'] / df_agg['FTE_DAYS_AVAILABLE'] * 100, 2)
    df_agg['NHSE_REGION_CODE'] = pd.NA
    df_agg['NHSE_REGION_NAME'] = pd.NA
    df_agg['BREAKDOWN_TYPE'] = 'MAJOR_STAFF_GROUPS'
    df_agg = df_agg[cols_order]
    return df_agg

def agg_minor_staff_groups(df, cols_to_aggregate, cols_order):
    """
    Creates a function to produce a dataframe of absence days available/ lost/ rates by minor staff group 
        
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        cols_order: Defined in the function agg_reporting_orgs as 'BREAKDOWN_TYPE', 'BREAKDOWN_VALUE', 
        'NHSE_REGION_CODE', 'NHSE_REGION_NAME',  'REPORTING_ORG_CODE', 'REPORTING_ORG_NAME',
        'WTE_DAYS_SICK_THIS_MONTH', 'WTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_RATE'
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by minor staff group 
    """
    logger.info("Producing the minor staff group aggregation")

    df_agg = (df.groupby('STAFF_GROUP_1_NAME', as_index=False)
            .agg(cols_to_aggregate)
            .rename({'STAFF_GROUP_1_NAME': 'BREAKDOWN_VALUE'}, axis=1))

    df_agg['SICKNESS_ABSENCE_RATE_PERCENT'] = round(df_agg['FTE_DAYS_LOST'] / df_agg['FTE_DAYS_AVAILABLE'] * 100, 2)
    df_agg['NHSE_REGION_CODE'] = pd.NA
    df_agg['NHSE_REGION_NAME'] = pd.NA
    df_agg['BREAKDOWN_TYPE'] = 'MINOR_STAFF_GROUPS'
    df_agg = df_agg[cols_order]
    return df_agg

def agg_medical_staff_grades(df, cols_to_aggregate, cols_order):
    """
    Creates a function to produce a dataframe of absence days available/ lost/ rates by minor staff grade 
     
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        cols_order: Defined in the function agg_reporting_orgs as 'BREAKDOWN_TYPE', 'BREAKDOWN_VALUE', 
        'NHSE_REGION_CODE', 'NHSE_REGION_NAME',  'REPORTING_ORG_CODE', 'REPORTING_ORG_NAME',
        'WTE_DAYS_SICK_THIS_MONTH', 'WTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_RATE'
    
    Outputs:
        Produces a dataframe of absence days available/ lost/ rates by minor staff grade 
     """
    logger.info("Producing the staff grade aggregation")

    df_agg = (df.groupby(['STAFF_GROUP_1_NAME', 'GRADE'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'GRADE': 'BREAKDOWN_VALUE'}, axis=1))

    df_agg['SICKNESS_ABSENCE_RATE_PERCENT'] = round(df_agg['FTE_DAYS_LOST'] / df_agg['FTE_DAYS_AVAILABLE'] * 100, 2)
    df_agg = df_agg.loc[df_agg['STAFF_GROUP_1_NAME'].isin(['HCHS Doctors'])]
    df_agg.drop(columns=['STAFF_GROUP_1_NAME'])
    df_agg['NHSE_REGION_CODE'] = pd.NA
    df_agg['NHSE_REGION_NAME'] = pd.NA
    df_agg['BREAKDOWN_TYPE'] = 'MINOR_STAFF_GRADES'
    df_agg = df_agg[cols_order]
    return df_agg

def agg_cluster_groups(df, cols_to_aggregate, cols_order):
    """
    Creates a function to produce a dataframe of absence days available/ lost/ rates by cluster group 
     
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        cols_order: Defined in the function agg_reporting_orgs as 'BREAKDOWN_TYPE', 'BREAKDOWN_VALUE', 
        'NHSE_REGION_CODE', 'NHSE_REGION_NAME',  'REPORTING_ORG_CODE', 'REPORTING_ORG_NAME',
        'WTE_DAYS_SICK_THIS_MONTH', 'WTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_RATE'
    
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by cluster group 
    """
    logger.info("Producing the cluster group aggregation")

    df_agg = (df.groupby(['CLUSTER_GROUP'], as_index=False)
                .agg(cols_to_aggregate)\
                .rename({'CLUSTER_GROUP': 'BREAKDOWN_VALUE'}, axis=1))

    df_agg['SICKNESS_ABSENCE_RATE_PERCENT'] = round(df_agg['FTE_DAYS_LOST'] / df_agg['FTE_DAYS_AVAILABLE'] * 100, 2)
    df_agg['NHSE_REGION_CODE'] = pd.NA
    df_agg['NHSE_REGION_NAME'] = pd.NA
    df_agg['BREAKDOWN_TYPE'] = 'ORGANISATION_TYPE'
    df_agg = df_agg[cols_order]
    return df_agg

def agg_reporting_orgs(df, cols_to_aggregate, month_date):
    """
    Creates a function to produce a dataframe of absence days available/ lost/ rates by organisation 
        
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        cols_order: Defined in the function agg_reporting_orgs as 'BREAKDOWN_TYPE', 'BREAKDOWN_VALUE', 
        'NHSE_REGION_CODE', 'NHSE_REGION_NAME',  'REPORTING_ORG_CODE', 'REPORTING_ORG_NAME',
        'WTE_DAYS_SICK_THIS_MONTH', 'WTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_RATE'
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by organisation 
    """
    logger.info("Producing the reporting orgs aggregation")
    
    cols_order = ['DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME',  'ORG_CODE', 'ORG_NAME', 'CLUSTER_GROUP',
                   'FTE_DAYS_LOST', 'FTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_PERCENT']

    df_agg = (df.groupby(['NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'ORG_NAME', 'CLUSTER_GROUP'], as_index=False)
                .agg(cols_to_aggregate))

    df_agg['SICKNESS_ABSENCE_RATE_PERCENT'] = round(df_agg['FTE_DAYS_LOST'] / df_agg['FTE_DAYS_AVAILABLE'] * 100, 2)
    df_agg['DATE'] = month_date
    df_agg['BREAKDOWN_TYPE'] = 'REPORTING_ORG'
    df_agg = df_agg[cols_order]
    df_agg = df_agg.rename(columns={'CLUSTER_GROUP': 'ORG_TYPE'})

    return df_agg

def create_absence_rates_breakdowns(df):
    """
    Creates a function that produces the final dataframe for the csv_1_output

    Inputs:
        df: dataframe derived from the SQL Server query
        
    Outputs: 
        Concatanates all of the aggregations listed in the inputs above into the csv_1_outputs dataframe 
    """
    logger.info("Getting ready to calculate all the absence breakdowns")
    cols_to_aggregate = {'FTE_DAYS_LOST': 'sum',
                        'FTE_DAYS_AVAILABLE': 'sum'}

    cols_order = ['BREAKDOWN_TYPE', 'BREAKDOWN_VALUE', 'NHSE_REGION_CODE',
                'NHSE_REGION_NAME', 'FTE_DAYS_LOST', 'FTE_DAYS_AVAILABLE', 'SICKNESS_ABSENCE_RATE_PERCENT']

    all_england = agg_all_england(df, cols_to_aggregate, cols_order)
    regional = agg_regions(df, cols_to_aggregate, cols_order)
    major_staff_groups = agg_major_staff_groups(df, cols_to_aggregate, cols_order)
    minor_staff_groups = agg_minor_staff_groups(df, cols_to_aggregate, cols_order)
    medical_staff_grades = agg_medical_staff_grades(df, cols_to_aggregate, cols_order)
    cluster_groups = agg_cluster_groups(df, cols_to_aggregate, cols_order)
    missing_cluster_group = pd.DataFrame([["ORGANISATION_TYPE", "Special Health Authority", pd.NA, pd.NA, pd.NA, pd.NA, 9999]], columns=cols_order)

    logger.info("Combining all of the aggregations")
    csv_1_outputs = pd.concat([all_england, regional, major_staff_groups,
                                minor_staff_groups, medical_staff_grades,
                                cluster_groups, missing_cluster_group])

    return csv_1_outputs

def create_org_absence_breakdowns(df, month_date):
    """
    Creates a function that produces the final panda dataframe for the csv_2_output

    Inputs:
        df: dataframe derived from the SQL Server query
        month_date: month_date as defined in config.toml file
       
    Outputs: 
        The final csv_2_output dataframe
    """
    cols_to_aggregate = {'FTE_DAYS_LOST': 'sum',
                        'FTE_DAYS_AVAILABLE': 'sum'}

    csv_2_outputs = agg_reporting_orgs(df, cols_to_aggregate, month_date)
    # suppress the data
    csv_2_outputs.loc[(csv_2_outputs["FTE_DAYS_AVAILABLE"] <= 330), "SICKNESS_ABSENCE_RATE_PERCENT"] = ''
    csv_2_outputs.loc[(csv_2_outputs["FTE_DAYS_AVAILABLE"] <= 330), "FTE_DAYS_LOST"] = ''
    csv_2_outputs.loc[(csv_2_outputs["FTE_DAYS_AVAILABLE"] <= 330), "FTE_DAYS_AVAILABLE"] = ''

    return csv_2_outputs
