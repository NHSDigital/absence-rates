import pandas as pd
import logging

logger = logging.getLogger(__name__)

def sql_query_reason_staff(database, mds_table, staff_in_post, org_master, ref_table, start_date, end_date):
    """
    Creates a function based on SQL code which pulls in the data for the sickness absence by reason and staff group table.
    
    Inputs:
        database: the SQL server where the absence data is stored
        mds_table: the minimum data set absence data table i.e. MDS_ABSENCE_YYYYMM
        staff_in_post: the staff in post data table i.e. Final_StaffInPost_YYYYMM_NEW_BASE_PROCESS
        org_master: the organisation reference table i.e. REF_ORG_MASTER
        ref_table: the occupation code reference table i.e. REF_CORP_WKFC_OCCUPATION_V01
        start_date: the last day of the month required in format YYYY-MM-DD
        end_date: the last day of the month required in format YYYY-MM-DD

    Output:
        A SQL server query wrapped in a f string containing the data to populate the sickness absence by reason and staff group table.
    """
    query = f"""
            select
                [Absence Category]          AS [ABSENCE_CATEGORY]
                ,[Attendance Reason]        AS [ATTENDANCE_REASON]
                ,[Wte Days Available]       AS [FTE_DAYS_AVAILABLE]
                ,[Wte Days Lost This Month] AS [WTE_DAYS_SICK_THIS_MONTH]
                ,a.[Tm End Date]            AS [TM_END_DATE]
                ,[Breed]                    AS [BREED]
                ,[Grade]                    AS [GRADE]
                ,[Staff Group]              AS [STAFF_GROUP]
                ,[MAIN_STAFF_GROUP_NAME]
                ,[STAFF_GROUP_1_NAME]
                ,CASE WHEN [Absence Category] = 'Sickness' THEN [Wte Days Lost This Month] ELSE 0 END as [FTE_DAYS_LOST]
            from [{database}].[dbo].[{mds_table}] a
            inner join [{org_master}] b
                on a.[ODS code] = b.[Current Org code]
            inner join [{staff_in_post}] c
                on a.[Unique Nhs Identifier] = c.[unique nhs identifier] and a.[Asg Number] = c.[Asg Number]
            inner join [{ref_table}] d
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

# Creating a variable of sickness absence reasons
absence_reasons = (
    'S10 Anxiety/stress/depression/other psychiatric illnesses', 'S11 Back Problems', 
    'S12 Other musculoskeletal problems', 'S13 Cold Cough Flu - Influenza', 
    'S14 Asthma', 'S15 Chest & respiratory problems',
    'S16 Headache / migraine', 'S17 Benign and malignant tumours cancers', 
    'S18 Blood disorders', 'S19 Heart cardiac & circulatory problems', 
    'S20 Burns poisoning frostbite hypothermia', 'S21 Ear nose throat \(ENT\)', 
    'S22 Dental and oral problems', 'S23 Eye problems', 
    'S24 Endocrine / glandular problems', 'S25 Gastrointestinal problems',
    'S26 Genitourinary & gynaecological disorders', 'S27 Infectious diseases', 
    'S28 Injury fracture', 'S29 Nervous system disorders', 
    'S30 Pregnancy related disorders', 'S31 Skin disorders', 
    'S32 Substance abuse', 'S98 Other known causes - not elsewhere classified', 
    'S99 Unknown causes / Not specified'
    )

# Creating a list of staff groups to ignore in the output
ignored_staff_group = (
    'General payments', 'Unknown', 'Non-funded staff'
)


def agg_all_staff_all_reasons(df, cols_to_aggregate):
    """
    Creates a function for use in creating the sickness absence rate for all staff groups and all absence reasons.
        
    Inputs:
        df: the data table, ESR-ABSENCE-YYYY-MM table from SQL.
        cols_to_aggregate: the columns used in the calculation of the sickness absence rate, FTE_DAYS_LOST and FTE_DAYS_AVAILABLE

    Output:
        A dataframe that contains an aggregation of data by all staff groups and all absence reasons.
    """
    logger.info("Producing the all staff, all reasons aggregation")

    df_agg = (df.groupby('TM_END_DATE', as_index=False)
            .agg(cols_to_aggregate))

    df_agg['BREAKDOWN_TYPE'] = 'All staff groups'
    df_agg['STAFF_GROUP'] = 'All staff groups'
    df_agg['REASON'] = 'ALL REASONS'

    return df_agg

def agg_all_staff_reasons(df, cols_to_aggregate):
    """
    Creates a function for use in creating the sickness absence rate for all staff groups by absence reasons.
        
    Inputs:
        df: the data table, ESR-ABSENCE-YYYY-MM table from SQL.
        cols_to_aggregate: the columns used in the calculation of the sickness absence rate, FTE_DAYS_LOST and FTE_DAYS_AVAILABLE

    Output:
        A dataframe that contains an aggregation of data of all staff groups by reason.
    """
    logger.info("Producing the all staff reasons aggregation")

    df_filtered = df[(df['ATTENDANCE_REASON'].str.contains('|'.join(absence_reasons)))] 

    df_agg = (df_filtered.groupby(['TM_END_DATE', 'ATTENDANCE_REASON'], as_index=False)
            .agg(cols_to_aggregate)
            .rename({'ATTENDANCE_REASON':'REASON'}, axis=1))

    df_agg['BREAKDOWN_TYPE'] = 'All staff groups'
    df_agg['STAFF_GROUP'] = 'All staff groups'

    return df_agg

def agg_minor_groups_all_reasons(df, cols_to_aggregate):
    """
    Creates a function for use in creating the sickness absence rate for all absence reasons by staff group.
        
    Inputs:
        df: the data table, ESR-ABSENCE-YYYY-MM table from SQL.
        cols_to_aggregate: the columns used in the calculation of the sickness absence rate, FTE_DAYS_LOST and FTE_DAYS_AVAILABLE 

    Output:
        A dataframe that contains an aggregation of data of all absence reasons by staff group.
    """
    logger.info("Producing the minor staff groups, all reasons aggregation")

    df_filtered = df[~df['STAFF_GROUP_1_NAME'].isin(ignored_staff_group)]

    df_agg = (df_filtered.groupby(['TM_END_DATE', 'STAFF_GROUP_1_NAME'], as_index=False)
            .agg(cols_to_aggregate))
   
    df_agg['BREAKDOWN_TYPE'] = 'MINOR STAFF GROUP'
    df_agg['STAFF_GROUP'] = df_agg['STAFF_GROUP_1_NAME']
    df_agg['REASON'] = 'ALL REASONS'

    return df_agg

def agg_major_groups_all_reasons(df, cols_to_aggregate):
    """
    Creates a function for use in creating the sickness absence rate for all absence reasons by main staff group.
        
    Inputs:
        df: the data table, ESR-ABSENCE-YYYY-MM table from SQL.
        cols_to_aggregate: the columns used in the calculation of the sickness absence rate, FTE_DAYS_LOST and FTE_DAYS_AVAILABLE

    Output:
        A dataframe that contains an aggregation of data of all absence reasons by main staff group.
    """
    logger.info("Producing the major staff groups, all reasons aggregation")

    df_filtered = df[~df['MAIN_STAFF_GROUP_NAME'].isin(ignored_staff_group)]

    df_agg = (df_filtered.groupby(['TM_END_DATE', 'MAIN_STAFF_GROUP_NAME'], as_index=False)
            .agg(cols_to_aggregate))
   
    df_agg['BREAKDOWN_TYPE'] = 'MAJOR STAFF GROUP'
    df_agg['STAFF_GROUP'] = df_agg['MAIN_STAFF_GROUP_NAME']
    df_agg['REASON'] = 'ALL REASONS'

    return df_agg

def agg_minor_grades_all_reasons(df, cols_to_aggregate):
    """
    Creates a function for use in creating the sickness absence rate for all absence reasons by grade.
        
    Inputs:
        df: the data table, ESR-ABSENCE-YYYY-MM table from SQL.
        cols_to_aggregate: the columns used in the calculation of the sickness absence rate, FTE_DAYS_LOST and FTE_DAYS_AVAILABLE

    Output:
        A dataframe that contains an aggregation of data of all absence reasons by grade.
    """
    logger.info("Producing the minor staff grades, all reasons aggregation")

    df = df[(df['BREED'].isin(['Med']))]

    df_filtered = df[~df['GRADE'].isin(ignored_staff_group)]

    df_agg = (df_filtered.groupby(['TM_END_DATE', 'GRADE'], as_index=False)
            .agg(cols_to_aggregate))
   
    df_agg['BREAKDOWN_TYPE'] = 'MINOR STAFF GRADES'
    df_agg['STAFF_GROUP'] = df_agg['GRADE']
    df_agg['REASON'] = 'ALL REASONS'

    return df_agg


def agg_minor_staff_groups(df, cols_to_aggregate):
    """
    Creates a function for use in creating the sickness absence rate by staff group and absence reason.
        
    Inputs:
        df: the data table, ESR-ABSENCE-YYYY-MM table from SQL.
        cols_to_aggregate: the columns used in the calculation of the sickness absence rate, FTE_DAYS_LOST and FTE_DAYS_AVAILABLE

    Output:
        A dataframe that contains an aggregation of data by staff groups and absence reason.
    """
    logger.info("Producing the minor staff groups reasons aggregation")

    df = df[(df['ATTENDANCE_REASON'].str.contains('|'.join(absence_reasons)))] 

    df_filtered = df[~df['STAFF_GROUP_1_NAME'].isin(ignored_staff_group)]

    df_agg = (df_filtered.groupby(['TM_END_DATE','STAFF_GROUP_1_NAME', 'ATTENDANCE_REASON'], as_index=False)
                .agg(cols_to_aggregate)
                .rename({'TM_END_DATE': 'DATE', 'ATTENDANCE_REASON': 'REASON', 'STAFF_GROUP_1_NAME':'STAFF_GROUP'}, axis=1))

    df_agg['BREAKDOWN_TYPE'] = 'MINOR STAFF GROUP'

    return df_agg


def agg_major_staff_groups(df, cols_to_aggregate):
    """
    Creates a function for use in creating the sickness absence rate by main staff group and absence reason.
        
    Inputs:
        df: the data table, ESR-ABSENCE-YYYY-MM table from SQL.
        cols_to_aggregate: the columns used in the calculation of the sickness absence rate, FTE_DAYS_LOST and FTE_DAYS_AVAILABLE

    Output:
        A dataframe that contains an aggregation of data by main staff groups and absence reason.
    """
    logger.info("Producing the major staff groups reasons aggregation")

    df = df[(df['ATTENDANCE_REASON'].str.contains('|'.join(absence_reasons)))] 
    df_filtered = df[~df['MAIN_STAFF_GROUP_NAME'].isin(ignored_staff_group)]

    df_agg = (df_filtered.groupby(['TM_END_DATE','MAIN_STAFF_GROUP_NAME','ATTENDANCE_REASON'], as_index=False)
                .agg(cols_to_aggregate)
                .rename({'MAIN_STAFF_GROUP_NAME': 'STAFF_GROUP','ATTENDANCE_REASON': 'REASON'}, axis=1))

    df_agg['BREAKDOWN_TYPE'] = 'MAJOR STAFF GROUP'

    return df_agg


def agg_minor_staff_grades(df, cols_to_aggregate):
    """
    Creates a function for use in creating the sickness absence rate by grade and absence reason.
        
    Inputs:
        df: the data table, ESR-ABSENCE-YYYY-MM table from SQL.
        cols_to_aggregate: the columns used in the calculation of the sickness absence rate, FTE_DAYS_LOST and FTE_DAYS_AVAILABLE 

    Output:
        A dataframe that contains an aggregation of data by grade and absence reason.
    """
    logger.info("Producing the minor staff grades reasons aggregation")

    df = df[(df['BREED'].isin(['Med']))]
    df = df[~df['GRADE'].isin(ignored_staff_group)]
    df_filtered = df[(df['ATTENDANCE_REASON'].str.contains('|'.join(absence_reasons)))] 

    df_agg = (df_filtered.groupby(['TM_END_DATE','GRADE', 'ATTENDANCE_REASON'], as_index=False)
                .agg(cols_to_aggregate)
                .rename({'GRADE': 'STAFF_GROUP','ATTENDANCE_REASON': 'REASON'}, axis=1))

    df_agg['BREAKDOWN_TYPE'] = 'MINOR STAFF GRADES'

    return df_agg


def create_reason_absence_breakdowns(df, month_date):
    """
    Creates a function summing the FTE days lost and FTE days available in preparation for the sickness absence by staff group and reason table.
    
    Inputs:
        df: the data table, ESR-ABSENCE-YYYY-MM table from SQL.
        month_date: the data month

    Output:
        A dataframe named reason_staff_data.
        This is aggregated by the breakdowns above e.g. by staff group, by reason etc.
    """
    logger.info("Getting ready to calculate all the reason and staff breakdowns")

    cols_to_aggregate = {'FTE_DAYS_LOST': 'sum',
                        'FTE_DAYS_AVAILABLE': 'sum'}

    all_staff_all_reasons = agg_all_staff_all_reasons(df, cols_to_aggregate)
    all_staff_reasons = agg_all_staff_reasons(df, cols_to_aggregate)
    major_group_all_reasons = agg_major_groups_all_reasons(df, cols_to_aggregate)
    minor_groups_all_reasons = agg_minor_groups_all_reasons(df, cols_to_aggregate)
    minor_grades_all_reasons = agg_minor_grades_all_reasons(df, cols_to_aggregate)
    major_staff_groups = agg_major_staff_groups(df, cols_to_aggregate)
    minor_staff_groups = agg_minor_staff_groups(df, cols_to_aggregate)
    minor_staff_grades = agg_minor_staff_grades(df, cols_to_aggregate)

    cols_order = ['DATE', 'STAFF_GROUP', 'REASON', 'FTE_DAYS_LOST', 'FTE_DAYS_AVAILABLE']

    logger.info("Combining all of the aggregations")
    reason_staff_data = pd.concat([all_staff_all_reasons, all_staff_reasons, major_group_all_reasons, 
                                minor_groups_all_reasons, minor_grades_all_reasons, major_staff_groups, minor_staff_groups, minor_staff_grades], 
                                axis=0, join='outer', sort=True)
    reason_staff_data['DATE'] = month_date
    reason_staff_data = reason_staff_data[cols_order]

    return reason_staff_data
