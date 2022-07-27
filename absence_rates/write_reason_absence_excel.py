import pandas as pd
import logging
from helpers import get_config

logger = logging.getLogger(__name__)

config = get_config()
month_date = config['month_date']

# Creating a variable of sickness absence reasons
absence_reasons = (
    'S10 Anxiety/stress/depression/other psychiatric illnesses', 'S11 Back Problems', 
    'S12 Other musculoskeletal problems', 'S13 Cold Cough Flu - Influenza', 
    'S14 Asthma', 'S15 Chest & respiratory problems',
    'S16 Headache / migraine', 'S17 Benign and malignant tumours cancers', 
    'S18 Blood disorders', 'S19 Heart cardiac & circulatory problems', 
    'S20 Burns poisoning frostbite hypothermia', 'S21 Ear nose throat (ENT)', 
    'S22 Dental and oral problems', 'S23 Eye problems', 
    'S24 Endocrine / glandular problems', 'S25 Gastrointestinal problems',
    'S26 Genitourinary & gynaecological disorders', 'S27 Infectious diseases', 
    'S28 Injury fracture', 'S29 Nervous system disorders', 
    'S30 Pregnancy related disorders', 'S31 Skin disorders', 
    'S32 Substance abuse', 'S98 Other known causes - not elsewhere classified', 
    'S99 Unknown causes / Not specified'
)

def prepare_reason_table_1(csv_path):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    
    Inputs:
        csv_path: the location where the data is being pulled from.
        
    Output:
        A dataframe named df_rounded which contains valid sickness absence reasons, FTE days, sickness absence rates and rounds the values to 1 decimal place.
    """
    logger.info(f"Preparing data for excel reason and staff table 1")

    logger.info(f"Reading file from:\n{csv_path}")

    df = pd.read_csv(csv_path)

    # Filter the dataframe to only include the current absence_reasons
    df_filtered = df[(df['REASON'].isin(absence_reasons))]

    # Set the index for the filtered dataframe 
    df_filtered_with_index = df_filtered.set_index(['DATE', 'STAFF_GROUP', 'REASON']).drop(columns=['FTE_DAYS_AVAILABLE'])
    # Return the dataframe with the breakdown values (reasons for absence) as the column
    df_unstacked = df_filtered_with_index.unstack('REASON')

    # Create a dataframe with only the ALL REASONS breakdown value in
    df_all_reasons = df[df['REASON'] == 'ALL REASONS']
    # Index the dataframe so that the only column in the dataframe is the FTE_DAYS_LOST
    all_reasons_sick_days_with_index = df_all_reasons.set_index(['DATE', 'STAFF_GROUP'])['FTE_DAYS_LOST']

    # Divide the FTE_DAYS_LOST in the full dataframe by the ALL REASONS FTE_DAYS_LOST and times by 100 to get the percentage of fte days lost by each absence reason
    df_fte_days_lost = (df_unstacked['FTE_DAYS_LOST'].divide(all_reasons_sick_days_with_index, axis='index')) * 100

    # Fill in null values with 0
    df_fill_null = df_fte_days_lost.fillna(0)

    # Round the values to 1 decimal places
    df_rounded = df_fill_null.round(1)

    return df_rounded


def prepare_reason_table_1_1(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from. 

    Output:
        A dataframe containing data for Table 1_1 tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the All staff groups row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_1")

    logger.info(f"Reading df:\n{df.head(1)}")

    # Get all of the values in a row with the corresponding DATE, BREAKDOWN_TYPE and STAFF_GROUP
    # Returning the row as a Series using loc:
    # This means that we are accessing a group of columns by the index labels inputted and returning the particular row of data we want
    # Get more information about loc: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.loc.html
    df_table_1_1 = df.loc[[(month_date, 'All staff groups')], :]
    df_table_1_1.to_csv('df.csv')
    return df_table_1_1

def prepare_reason_table_1_2(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.
        
    Output:
        A dataframe containing data for Table 1_2 tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Professionally qualified clinical staff row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_2")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_2 = df.loc[[(month_date, 'Professionally qualified clinical staff')], :]

    return df_table_1_2


def prepare_reason_table_1_3_a(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from. 

    Output:
        A dataframe containing data for Table 1_3_a tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the HCHS doctor row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_a")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_a = df.loc[[(month_date, 'HCHS Doctors')], :]

    return df_table_1_3_a

def prepare_reason_table_1_3_b(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from. 

    Output:
        A dataframe containing data for Table 1_3_b tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Consultant grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_b")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_b = df.loc[[(month_date, 'Consultant')], :]

    return df_table_1_3_b

def prepare_reason_table_1_3_c(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_3_c tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Associate Specialist grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_c")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_c = df.loc[[(month_date, 'Associate Specialist')], :]

    return df_table_1_3_c

def prepare_reason_table_1_3_d(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_3_d tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Specialty Doctor grade row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_d")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_d = df.loc[[(month_date, 'Specialty Doctor')], :]

    return df_table_1_3_d

def prepare_reason_table_1_3_e(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_3_e tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Staff Grade grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_e")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_e = df.loc[[(month_date, 'Staff Grade')], :]

    return df_table_1_3_e

def prepare_reason_table_1_3_f(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_3_f tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Specialty Registrar grade row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_f")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_f = df.loc[[(month_date, 'Specialty Registrar')], :]

    return df_table_1_3_f

def prepare_reason_table_1_3_g(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_3_g tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Core Training grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_g")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_g = df.loc[[(month_date, 'Core Training')], :]

    return df_table_1_3_g

def prepare_reason_table_1_3_h(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_3_h tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Foundation Doctor Year 2 grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_h")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_h = df.loc[[(month_date, 'Foundation Doctor Year 2')], :]

    return df_table_1_3_h

def prepare_reason_table_1_3_i(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_3_i tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Foundation Doctor Year 1 grade row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_i")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_i = df.loc[[(month_date, 'Foundation Doctor Year 1')], :]

    return df_table_1_3_i

def prepare_reason_table_1_3_j(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.
        
    Output:
        A dataframe containing data for Table 1_3_j tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the HPCA grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_j")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_j = df.loc[[(month_date, 'Hospital Practitioner / Clinical Assistant')], :]

    return df_table_1_3_j

def prepare_reason_table_1_3_k(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_3_k tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Other and Local Doctor grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_3_k")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_3_k = df.loc[[(month_date, 'Other and Local HCHS Doctor Grades')], :]

    return df_table_1_3_k

def prepare_reason_table_1_4_a(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from. 

    Output:
        A dataframe containing data for Table 1_4_a tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Nurses & HVs row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_4_a")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_4_a = df.loc[[(month_date, 'Nurses & health visitors')], :]

    return df_table_1_4_a

def prepare_reason_table_1_4_b(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_4_b tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Midwives row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_4_b")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_4_b = df.loc[[(month_date, 'Midwives')], :]

    return df_table_1_4_b

def prepare_reason_table_1_4_c(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_4_c tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Ambulance staff row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_4_c")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_4_c = df.loc[[(month_date, 'Ambulance staff')], :]

    return df_table_1_4_c

def prepare_reason_table_1_4_d(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_4_d tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the ST&T staff row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_4_d")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_4_d = df.loc[[(month_date, 'Scientific, therapeutic & technical staff')], :]

    return df_table_1_4_d

def prepare_reason_table_1_5_a(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_5_a tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Support to clinical staff row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_5_a")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_5_a = df.loc[[(month_date, 'Support to clinical staff')], :]

    return df_table_1_5_a

def prepare_reason_table_1_5_b(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from. 

    Output:
        A dataframe containing data for Table 1_5_b tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Support to doctors, nurses and midwives row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_5_b")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_5_b = df.loc[[(month_date, 'Support to doctors, nurses & midwives')], :]

    return df_table_1_5_b

def prepare_reason_table_1_5_c(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_5_c tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Support to ambulance staff row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_5_c")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_5_c = df.loc[[(month_date, 'Support to ambulance staff')], :]

    return df_table_1_5_c

def prepare_reason_table_1_5_d(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_5_d tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Support to ST&T staff row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_5_d")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_5_d = df.loc[[(month_date, 'Support to ST&T staff')], :]

    return df_table_1_5_d

def prepare_reason_table_1_6_a(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_6_a tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the NHS infrastructure staff row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_6_a")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_6_a = df.loc[[(month_date, 'NHS infrastructure support')], :]

    return df_table_1_6_a

def prepare_reason_table_1_6_b(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_6_b tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Central functions row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_6_b")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_6_b = df.loc[[(month_date, 'Central functions')], :]

    return df_table_1_6_b

def prepare_reason_table_1_6_c(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_6_c tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Hotel, property & estates row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_6_c")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_6_c = df.loc[[(month_date, 'Hotel, property & estates')], :]

    return df_table_1_6_c

def prepare_reason_table_1_6_d(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_6_d tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Senior managers row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_6_d")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_6_d = df.loc[[(month_date, 'Senior managers')], :]

    return df_table_1_6_d

def prepare_reason_table_1_6_e(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_6_e tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Managers row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_6_e")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_6_e = df.loc[[(month_date, 'Managers')], :]

    return df_table_1_6_e

def prepare_reason_table_1_7(df):
    """
    Creates a function to prepare the data for Table 1 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 1 using a series of tags e.g. 1_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 1_7 tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Other staff or those with unknown classification row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 1_7")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_1_7 = df.loc[[(month_date, 'Other staff or those with unknown classification')], :]

    return df_table_1_7


def prepare_reason_table_2(csv_path):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    
    Inputs:
        csv_path: the location where the data is being pulled from.
       
    Output:
        A dataframe named df_rounded which contains valid sickness absence reasons, FTE days and rounds the values to 1 decimal place.
    """
    logger.info(f"Preparing data for excel reason and staff table 2")
    logger.info(f"Reading file from:\n{csv_path}")

    df = pd.read_csv(csv_path)

    # Add ALL REASONS to the absence_reasons tuple as an accepted reason - this is done here as adding it at the start causes a problem in Table 1
    absence_reasons_all = ('ALL REASONS',) + absence_reasons

    # Filter the dataframe to only include the current absence_reasons
    df_filtered = df[(df['REASON'].isin(absence_reasons_all))]
    # Replace ALL REASONS with ALL REASONS DAYS LOST as df_filtered will only be looking at the FTE_DAYS_LOST (days lost)
    df_filtered['REASON'] = df_filtered['REASON'].str.replace('ALL REASONS', 'ALL REASONS DAYS LOST')

    # Create a dataframe with only the ALL REASONS breakdown value in
    df_all_reasons = df[df['REASON'] == 'ALL REASONS']

    # Remove the FTE_DAYS_LOST column as this is not needed to get the days available 
    df_without_sick_days = df_all_reasons.drop(columns="FTE_DAYS_LOST")
    # Rename the FTE_DAYS_AVAILABLE to FTE_DAYS_LOST so that we can join together the days available with the days lost in one dataframe, later in the script 
    wte_days_renamed = df_without_sick_days.rename(columns={'FTE_DAYS_AVAILABLE':'FTE_DAYS_LOST'})
    # Rename the ALL REASONS value to ALL REASONS DAYS AVAILABLE as this dataframe only looks at the FTE_DAYS_AVAILABLE (days available)
    wte_days_renamed['REASON'] = wte_days_renamed['REASON'].str.replace('ALL REASONS', 'ALL REASONS DAYS AVAILABLE')

    # Join together the wte_days_renamed and df_filtered dataframes
    df_concatenated = pd.concat([wte_days_renamed, df_filtered])

    # Order the dataframe by STAFF_GROUP first so that the ALL REASONS values are in the correct groups then order by REASON so that the order matches the template
    df_ordered = df_concatenated.sort_values(by=['STAFF_GROUP', 'REASON'])

    # Index the dataframe so that the only column in the dataframe is the FTE_DAYS_LOST
    df_ordered_with_index = df_ordered.set_index(['DATE', 'STAFF_GROUP','REASON'])['FTE_DAYS_LOST']
    # Return the dataframe with the breakdown values (reasons for absence) as the columns
    df_ordered_with_index_unstacked = df_ordered_with_index.unstack('REASON')
    # Fill any null spaces with a dash
    df_fill_null = df_ordered_with_index_unstacked.fillna('-')
    # Round the values to 0 decimal places
    df_rounded = df_fill_null.round()

    return df_rounded

def prepare_reason_table_2_1(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_1 tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the All staff groups row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_1")

    logger.info(f"Reading df:\n{df.head(1)}")

    # Get all of the values for the row containing the correct start date, All staff groups and All staff groups
    df_table_2_1 = df.loc[[(month_date, 'All staff groups')], :]

    return df_table_2_1

def prepare_reason_table_2_2(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_2 tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Professionally qualified clinical staff row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_2")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_2 = df.loc[[(month_date, 'Professionally qualified clinical staff')], :]

    return df_table_2_2

def prepare_reason_table_2_3_a(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_a tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the HCHS doctors row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_a")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_a = df.loc[[(month_date, 'HCHS Doctors')], :]

    return df_table_2_3_a

def prepare_reason_table_2_3_b(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_b tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Consultant grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_b")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_b = df.loc[[(month_date, 'Consultant')], :]

    return df_table_2_3_b

def prepare_reason_table_2_3_c(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_c tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Associate Specialist grade row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_c")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_c = df.loc[[(month_date, 'Associate Specialist')], :]

    return df_table_2_3_c

def prepare_reason_table_2_3_d(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_d tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Specialty Doctor grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_d")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_d = df.loc[[(month_date, 'Specialty Doctor')], :]

    return df_table_2_3_d

def prepare_reason_table_2_3_e(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_e tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Staff Grade grade row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_e")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_e = df.loc[[(month_date, 'Staff Grade')], :]

    return df_table_2_3_e

def prepare_reason_table_2_3_f(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_f tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Specialty Registrar grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_f")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_f = df.loc[[(month_date, 'Specialty Registrar')], :]

    return df_table_2_3_f

def prepare_reason_table_2_3_g(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_g tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Core Training grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_g")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_g = df.loc[[(month_date, 'Core Training')], :]

    return df_table_2_3_g

def prepare_reason_table_2_3_h(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_h tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Foundation Doctor Year 2 grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_h")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_h = df.loc[[(month_date, 'Foundation Doctor Year 2')], :]

    return df_table_2_3_h

def prepare_reason_table_2_3_i(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_i tag of the NHS Sickness Absence by staff group and reason table. 
        This tag populates the Foundation Doctor Year 1 grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_i")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_i = df.loc[[(month_date, 'Foundation Doctor Year 1')], :]

    return df_table_2_3_i

def prepare_reason_table_2_3_j(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_j tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the HPCA grade row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_j")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_j = df.loc[[(month_date, 'Hospital Practitioner / Clinical Assistant')], :]

    return df_table_2_3_j

def prepare_reason_table_2_3_k(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_3_k tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Other and Local grade row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_3_k")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_3_k = df.loc[[(month_date, 'Other and Local HCHS Doctor Grades')], :]

    return df_table_2_3_k

def prepare_reason_table_2_4_a(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.

    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_4_a tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Nurses & HVs row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_4_a")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_4_a = df.loc[[(month_date, 'Nurses & health visitors')], :]

    return df_table_2_4_a

def prepare_reason_table_2_4_b(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.

    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_4_b tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Midwives row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_4_b")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_4_b = df.loc[[(month_date, 'Midwives')], :]

    return df_table_2_4_b

def prepare_reason_table_2_4_c(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_4_c tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Ambulance staff row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_4_c")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_4_c = df.loc[[(month_date, 'Ambulance staff')], :]

    return df_table_2_4_c

def prepare_reason_table_2_4_d(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_4_d tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the ST&T staff row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_4_d")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_4_d = df.loc[[(month_date, 'Scientific, therapeutic & technical staff')], :]

    return df_table_2_4_d

def prepare_reason_table_2_5_a(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_5_a tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Support to clinical staff row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_5_a")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_5_a = df.loc[[(month_date, 'Support to clinical staff')], :]

    return df_table_2_5_a

def prepare_reason_table_2_5_b(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_5_b tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Support to doctors, nurses & midwives row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_5_b")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_5_b = df.loc[[(month_date, 'Support to doctors, nurses & midwives')], :]

    return df_table_2_5_b


def prepare_reason_table_2_5_c(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_5_c tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Support to ambulance staff row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_5_c")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_5_c = df.loc[[(month_date, 'Support to ambulance staff')], :]

    return df_table_2_5_c


def prepare_reason_table_2_5_d(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_5_d tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Support to ST&T staff row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_5_d")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_5_d = df.loc[[(month_date, 'Support to ST&T staff')], :]

    return df_table_2_5_d



def prepare_reason_table_2_6_a(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_6_a tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the NHS infrastructure staff row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_6_a")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_6_a = df.loc[[(month_date, 'NHS infrastructure support')], :]

    return df_table_2_6_a

def prepare_reason_table_2_6_b(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_6_b tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Central functions row. 
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_6_b")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_6_b = df.loc[[(month_date, 'Central functions')], :]

    return df_table_2_6_b

def prepare_reason_table_2_6_c(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_6_c tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Hotel, property & estates staff row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_6_c")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_6_c = df.loc[[(month_date, 'Hotel, property & estates')], :]

    return df_table_2_6_c

def prepare_reason_table_2_6_d(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_6_d tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Senior manager row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_6_d")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_6_d = df.loc[[(month_date, 'Senior managers')], :]

    return df_table_2_6_d

def prepare_reason_table_2_6_e(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from. 

    Output:
        A dataframe containing data for Table 2_6_e tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Managers row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_6_e")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_6_e = df.loc[[(month_date, 'Managers')], :]

    return df_table_2_6_e

def prepare_reason_table_2_7(df):
    """
    Creates a function to prepare the data for Table 2 of the NHS Sickness Absence by staff group and reason table.
    Writing to Table 2 using a series of tags e.g. 2_1.
    
    Inputs:
        df: the dataframe where the data is being pulled from.

    Output:
        A dataframe containing data for Table 2_7 tag of the NHS Sickness Absence by staff group and reason table.
        This tag populates the Other staff or those with unknown classification row.
    """
    logger.info(f"Preparing data for excel tag in the reason and staff table 2_7")

    logger.info(f"Reading df:\n{df.head(1)}")
    df_table_2_7 = df.loc[[(month_date, 'Other staff or those with unknown classification')], :]

    return df_table_2_7
