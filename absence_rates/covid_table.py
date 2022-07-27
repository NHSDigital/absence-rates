import pandas as pd
import logging

logger = logging.getLogger(__name__)

def sql_query_covid_data(database, mds_table, staff_in_post, org_master, ref_table, start_date, end_date):
    """
    Creates a function to select the required fields for the covid data

    Inputs:
        database: database name as defined in config.toml file
        mds_table: base MDS absence data table name as defined in the config.toml file
        staff_in_post: staff in post data table to link to as defined in the config.toml file
        org_master: Organisation ref table as defined in config.toml file
        ref_table: Occupation code ref table as defined in config.toml file
        start_date: Start date of data as defined in config.toml file
        end_date: End date of data as defined in config.toml file
        
    Output:
        SQL query which selects fields to be used in a base set used to create the covid absence data
        
    """
    logger.info("Preparing the base data SQL query for COVID data.")
    query = f"""
            select
                [Absence Category]          AS [ABSENCE_CATEGORY]
                ,[Related Reason]           AS [RELATED_REASON]
                ,[Wte Days Available]       AS [FTE_DAYS_AVAILABLE]
                ,[Wte Days Lost This Month] AS [WTE_DAYS_LOST_THIS_MONTH]
                ,a.[Tm End Date]            AS [TM_END_DATE]
                ,[NHSE_Region_Code]         AS [NHSE_REGION_CODE]
                ,[NHSE_Region_Name]         AS [NHSE_REGION_NAME]
                ,[Reporting Org Code]       AS [ORG_CODE]
                ,[Breed]                    AS [BREED]
                ,[Grade]                    AS [GRADE]
                ,[MAIN_STAFF_GROUP_NAME]
                ,[STAFF_GROUP_1_NAME]
                ,CASE WHEN [Absence Category] = 'Sickness' THEN [Wte Days Lost This Month] ELSE 0 END as [FTE_DAYS_LOST]
                ,CASE WHEN [Absence Category] = 'Sickness' AND [Related Reason] like 'Coronavirus (COVID-19)%' THEN [Wte Days Lost This Month] ELSE 0 END AS [FTE_DAYS_LOST_COVID]
            from [{database}].[dbo].[{mds_table}] a
            inner join [{org_master}] b
                on a.[ODS code] = b.[Current Org code]
            left join [{staff_in_post}] c
                on a.[Unique Nhs Identifier] = c.[unique nhs identifier] and a.[Asg Number] = c.[Asg Number]
            left join [{ref_table}] d
                on c.[Occupation Code] = d.[OCC_CODE]
            where (b.[End Date] >= '{end_date}'
            or b.[End Date] is null)
            and b.[Start Date] < '{start_date}'
            and b.[EnglandWales] = 'E'
            and b.[Reporting Org code] not in ('8HK67','8J318','8J149','NL1')
            and b.[Reporting Org code] not like '[5Q]%'
            and (d.[END_DATE_PUBLICATION] >= '{end_date}'
            or d.[END_DATE_PUBLICATION] is null)
            and d.[START_DATE_PUBLICATION] < '{start_date}'
            """
    return query

def agg_all_england_all_staff(df, cols_to_aggregate):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates at a total England level 
     
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates at a total England level 
    
    """
    logger.info("Producing the all England COVID aggregation")

    df_agg = (df.groupby('TM_END_DATE', as_index=False)
            .agg(cols_to_aggregate)
            .rename({'TM_END_DATE': 'BREAKDOWN_VALUE'}, axis=1))
 
    df_agg['NHSE_REGION_CODE'] = 'All NHSE regions'
    df_agg['NHSE_REGION_NAME'] = 'All NHSE regions'
    df_agg['ORG_CODE'] = 'All organisations'
    df_agg['ORG_NAME'] = 'All organisations'
    df_agg['STAFF_GROUP'] = 'All staff groups'
    
    return df_agg

def agg_all_england_minor_staff_groups(df, cols_to_aggregate):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates by minor staff group 
      
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by minor staff group 
    """
    logger.info("Producing the all England COVID minor staff aggregation")

    df_filtered = df[~df['STAFF_GROUP_1_NAME'].isin(['General payments', 'Unknown', 'Non-funded staff'])]

    df_agg = (df_filtered.groupby(['TM_END_DATE', 'STAFF_GROUP_1_NAME'], as_index=False)
                .agg(cols_to_aggregate)
                .rename({'TM_END_DATE': 'BREAKDOWN_VALUE_1'}, axis=1))
   
    df_agg['NHSE_REGION_CODE'] = 'All NHSE regions'
    df_agg['NHSE_REGION_NAME'] = 'All NHSE regions'
    df_agg['ORG_CODE'] = 'All organisations'
    df_agg['ORG_NAME'] = 'All organisations'
    df_agg['STAFF_GROUP'] = df_agg['STAFF_GROUP_1_NAME']
    
    return df_agg

def agg_all_england_major_staff_groups(df, cols_to_aggregate):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates by major staff group 
           
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by major staff group 
    """
    logger.info("Producing the all England COVID major staff aggregation")

    df_agg = (df.groupby(['TM_END_DATE', 'MAIN_STAFF_GROUP_NAME'], as_index=False)
                .agg(cols_to_aggregate)
                .rename({'TM_END_DATE': 'BREAKDOWN_VALUE_1'}, axis=1))
   
    df_agg['NHSE_REGION_CODE'] = 'All NHSE regions'
    df_agg['NHSE_REGION_NAME'] = 'All NHSE regions'
    df_agg['ORG_CODE'] = 'All organisations'
    df_agg['ORG_NAME'] = 'All organisations'
    df_agg['STAFF_GROUP'] = df_agg['MAIN_STAFF_GROUP_NAME']
    
    return df_agg
    
def agg_all_england_medical_staff_grades(df, cols_to_aggregate):
    """
    Creates a function to produce a dataframe of absence days available/ lost/ rates by medical grade 

    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by medical grade 
 
    """
    logger.info("Producing the all England COVID medical staff grade aggregation")

    df_filtered = df[(df['BREED'].isin(['Med']))]

    df_agg = (df_filtered.groupby(['TM_END_DATE', 'GRADE'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'TM_END_DATE': 'BREAKDOWN_VALUE_1'}, axis=1))
   
    df_agg['NHSE_REGION_CODE'] = 'All NHSE regions'
    df_agg['NHSE_REGION_NAME'] = 'All NHSE regions'
    df_agg['ORG_CODE'] = 'All organisations'
    df_agg['ORG_NAME'] = 'All organisations'
    df_agg['STAFF_GROUP'] = df_agg['GRADE']
    
    return df_agg

def agg_regions_all_staff(df, cols_to_aggregate):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates by NHSE Region     
    
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        
    Outputs: 
       Produces a dataframe of absence days available/ lost/ rates by NHSE Region     
    """
    logger.info("Producing the region COVID aggregation")

    df_agg = (df.groupby(['TM_END_DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'TM_END_DATE': 'BREAKDOWN_VALUE'}, axis=1))
 
    df_agg['ORG_CODE'] = 'All organisations'
    df_agg['ORG_NAME'] = 'All organisations'
    df_agg['STAFF_GROUP'] = 'All staff groups'
    
    return df_agg

def agg_regions_minor_staff_groups(df, cols_to_aggregate):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates by NHSE Region and minor staff group     
    
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by NHSE Region and minor staff group     
    
    """
    logger.info("Producing the region COVID minor staff aggregation")

    df_filtered = df[~df['STAFF_GROUP_1_NAME'].isin(['General payments', 'Unknown', 'Non-funded staff'])]

    df_agg = (df_filtered.groupby(['TM_END_DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'STAFF_GROUP_1_NAME'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'TM_END_DATE': 'BREAKDOWN_VALUE_1'}, axis=1))
   
    df_agg['ORG_CODE'] = 'All organisations'
    df_agg['ORG_NAME'] = 'All organisations'
    df_agg['STAFF_GROUP'] = df_agg['STAFF_GROUP_1_NAME']
    
    return df_agg

def agg_regions_major_staff_groups(df, cols_to_aggregate):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates by NHSE Region and major staff group
           
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by NHSE Region and major staff group
           
    """
    logger.info("Producing the region COVID major staff aggregation")

    df_agg = (df.groupby(['TM_END_DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'MAIN_STAFF_GROUP_NAME'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'TM_END_DATE': 'BREAKDOWN_VALUE_1'}, axis=1))
   
    df_agg['ORG_CODE'] = 'All organisations'
    df_agg['ORG_NAME'] = 'All organisations'
    df_agg['STAFF_GROUP'] = df_agg['MAIN_STAFF_GROUP_NAME']
    
    return df_agg
   
def agg_regions_medical_staff_grades(df, cols_to_aggregate):
    """
    Creates a function to produce a dataframe of absence days available/ lost/ rates by NHSE Region and medical grade 

    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        
    Outputs: 
       Produces a dataframe of absence days available/ lost/ rates by NHSE Region and medical grade 

    """    
    logger.info("Producing the region COVID medical staff grade aggregation")

    df_filtered = df[(df['BREED'].isin(['Med']))]

    df_agg = (df_filtered.groupby(['TM_END_DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'GRADE'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'TM_END_DATE': 'BREAKDOWN_VALUE_1'}, axis=1))
   
    df_agg['ORG_CODE'] = 'All organisations'
    df_agg['ORG_NAME'] = 'All organisations'
    df_agg['STAFF_GROUP'] = df_agg['GRADE']
    
    return df_agg

def agg_orgs_all_staff(df, cols_to_aggregate):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates by Organisation     
    
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
        
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by Organisation     
    
    """
    logger.info("Producing the orgs COVID aggregation")

    df_agg = (df.groupby(['TM_END_DATE', 'NHSE_REGION_CODE', 'ORG_CODE', 'NHSE_REGION_NAME'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'TM_END_DATE': 'BREAKDOWN_VALUE'}, axis=1))
 
    df_agg['STAFF_GROUP'] = 'All staff groups'
    
    return df_agg

def agg_orgs_minor_staff_groups(df, cols_to_aggregate):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates by Organisation and minor staff group     
    
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
            
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by Organisation and minor staff group     
    
    """
    logger.info("Producing the orgs COVID minor staff group aggregation")

    df_filtered = df[~df['STAFF_GROUP_1_NAME'].isin(['General payments', 'Unknown', 'Non-funded staff'])]

    df_agg = (df_filtered.groupby(['TM_END_DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'STAFF_GROUP_1_NAME'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'TM_END_DATE': 'BREAKDOWN_VALUE_1'}, axis=1))
   
    df_agg['STAFF_GROUP'] = df_agg['STAFF_GROUP_1_NAME']
    
    return df_agg

def agg_orgs_major_staff_groups(df, cols_to_aggregate):
    """ 
    Creates a function to produce a dataframe of absence days available/ lost/ rates by NHSE Region and major staff group
            
    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
            
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by NHSE Region and major staff group
            
    """
    logger.info("Producing the orgs COVID major staff aggregation")

    df_agg = (df.groupby(['TM_END_DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'MAIN_STAFF_GROUP_NAME'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'TM_END_DATE': 'BREAKDOWN_VALUE_1'}, axis=1))
   
    df_agg['STAFF_GROUP'] = df_agg['MAIN_STAFF_GROUP_NAME']
    
    return df_agg

def agg_orgs_medical_staff_grades(df, cols_to_aggregate):
    ## TODO maybe rename the function to medical staff grade?
    """
    Creates a function to produce a dataframe of absence days available/ lost/ rates by Organisation and medical grade 

    Inputs:
        df: dataframe derived from the SQL Server query
        cols_to_aggregate: contains the sum of WTE_DAYS_SICK_THIS_MONTH and the sum of WTE_DAYS_AVAILABLE.
            
    Outputs: 
        Produces a dataframe of absence days available/ lost/ rates by Organisation and medical grade 
    """
    logger.info("Producing the orgs COVID medical staff grades aggregation")

    df_filtered = df[(df['BREED'].isin(['Med']))]

    df_agg = (df_filtered.groupby(['TM_END_DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'GRADE'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'TM_END_DATE': 'BREAKDOWN_VALUE_1'}, axis=1))
   
    df_agg['STAFF_GROUP'] = df_agg['GRADE']
    
    return df_agg

def create_covid_breakdowns(df):
    """
    Creates a function that produces the final COVID data output dataframe for England and NHSE Region
    
    Inputs:
        df: dataframe derived from the SQL Server query
        
    Outputs: 
        Produces the final COVID data output dataframe for England and NHSE Region 
    """
    logger.info("Getting ready to calculate all the COVID breakdowns")

    cols_to_aggregate = {'FTE_DAYS_LOST': 'sum',
                        'FTE_DAYS_AVAILABLE': 'sum',
                        'FTE_DAYS_LOST_COVID': 'sum'}

    cols_order = ['NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'ORG_NAME', 'STAFF_GROUP', 'FTE_DAYS_AVAILABLE', 'FTE_DAYS_LOST', 'FTE_DAYS_LOST_COVID']

    all_england_staff = agg_all_england_all_staff(df, cols_to_aggregate)
    all_england_major_staff_groups = agg_all_england_major_staff_groups(df, cols_to_aggregate)
    all_england_minor_staff_groups = agg_all_england_minor_staff_groups(df, cols_to_aggregate)
    all_england_medical_staff_grades = agg_all_england_medical_staff_grades(df, cols_to_aggregate)
    all_regions_staff = agg_regions_all_staff(df, cols_to_aggregate)
    all_regions_major_staff_groups = agg_regions_major_staff_groups(df, cols_to_aggregate)
    all_regions_minor_staff_groups = agg_regions_minor_staff_groups(df, cols_to_aggregate)
    all_regions_medical_staff_grades = agg_regions_medical_staff_grades(df, cols_to_aggregate)

    logger.info("Combining all of the aggregations")
    covid_data = pd.concat([all_england_staff, all_england_major_staff_groups, all_england_minor_staff_groups, all_england_medical_staff_grades, all_regions_staff,
                    all_regions_major_staff_groups, all_regions_minor_staff_groups, all_regions_medical_staff_grades], axis=0, join='outer', sort=True)

    covid_data = covid_data[cols_order]

    return covid_data

def create_covid_orgs_breakdowns(df):
    """
    Creates a function that produces the final COVID data output dataframe at an Organisation level
    
    Inputs:
        df: dataframe derived from the SQL Server query
        
    Outputs: 
        Produces the final COVID data output dataframe at an Organisation level
    
    """
    logger.info("Getting ready to calculate all the COVID org breakdowns")

    cols_to_aggregate = {'FTE_DAYS_LOST': 'sum',
                        'FTE_DAYS_AVAILABLE': 'sum',
                        'FTE_DAYS_LOST_COVID': 'sum'}

    cols_order = ['NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE',
                    'STAFF_GROUP', 'FTE_DAYS_AVAILABLE', 'FTE_DAYS_LOST', 'FTE_DAYS_LOST_COVID']

    all_orgs_staff = agg_orgs_all_staff(df, cols_to_aggregate)
    all_orgs_major_staff_groups = agg_orgs_major_staff_groups(df, cols_to_aggregate)
    all_orgs_minor_staff_groups = agg_orgs_minor_staff_groups(df, cols_to_aggregate)
    all_orgss_medical_staff_grades = agg_orgs_medical_staff_grades(df, cols_to_aggregate)

    logger.info("Combining all of the aggregations")
    covid_org_data = pd.concat([all_orgs_staff, all_orgs_major_staff_groups,
                    all_orgs_minor_staff_groups, all_orgss_medical_staff_grades], axis=0, join='outer', sort=True)

    covid_org_data = covid_org_data[cols_order]

    return covid_org_data
    
# Join to get the latest org name
def covid_joined_table(df, df_query):
    """
    Creates a function that to join the original COVID data output dataframe at an Organisation level to the latest org name data
    
    Inputs:
        df: dataframe containg aggregates for NHS Organisations.
        df_query: a table containing the latest org names produced as a result of the sql_latest_org_name function.   
               
    Outputs: 
        Produces the COVID Organisation level dataframe with up to date org name details
    
    """
    logger.info("Getting ready to join to the latest org data")

    cols_order = ['NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'ORG_NAME', 
                'STAFF_GROUP', 'FTE_DAYS_AVAILABLE', 'FTE_DAYS_LOST', 'FTE_DAYS_LOST_COVID']
    

    # filter columns to keep df_query from causing conflicts with duplicate columns
    df_cut = df_query[['ORG_CODE', 'ORG_NAME']]

    # join the two dfs
    covid_csv_outputs = df.merge(df_cut, how='inner', on='ORG_CODE')
    covid_csv_outputs = covid_csv_outputs[cols_order]

    return covid_csv_outputs

def covid_final_table(df, df_2, month_date):
    """
    Creates a function that to union all the covid dataframes created above into one dataframe

    Inputs:
            df: dataframe containing aggregates for England and NHSE Regions.
            df_2: dataframe containing the latest information and aggregates for NHS organisations, derived from covid_joined_table().
            month_date: : month_date as defined in config.toml file
                    
        Outputs: 
            Produces a unioned data into a final covid dataframe
    
    """
    logger.info("Getting ready to union all COVID data")

    cols_order = ['DATE', 'NHSE_REGION_CODE', 'NHSE_REGION_NAME', 'ORG_CODE', 'ORG_NAME', 
                'STAFF_GROUP', 'FTE_DAYS_AVAILABLE', 'FTE_DAYS_LOST', 'FTE_DAYS_LOST_COVID']

    covid_final_data = pd.concat([df,df_2], axis=0, join='outer', sort=True)

    covid_final_data['DATE'] = month_date

    covid_final_data = covid_final_data[cols_order]

    # suppress the data for FTE_DAYS_AVAILABLE values 330 or less
    covid_final_data.loc[(covid_final_data['FTE_DAYS_AVAILABLE'] <= 330), 'FTE_DAYS_LOST'] = '' 
    covid_final_data.loc[(covid_final_data['FTE_DAYS_AVAILABLE'] <= 330), 'FTE_DAYS_LOST_COVID'] = ''
    covid_final_data.loc[(covid_final_data['FTE_DAYS_AVAILABLE'] <= 330), 'FTE_DAYS_AVAILABLE'] = ''
    
    return covid_final_data
