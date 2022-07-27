"""
This script reads in the _RAW data and outputs the _PROCESSED data.

In the middle, it updates invalid occupation codes and does a few other bits of housekeeping.
"""

import pandas as pd
from data_connections import execute_sql


def read_occ_code_update_mappings(occ_codes_update_path):
    """
    Creates a function which will read in the list of updates that you want to make to the invalid occupation codes.

    Inputs:
        occ_codes_update_path: Path to the csv containing the updated occupation codes

    Outputs: 
        a dict of mappings
    """

    try:
        occ_codes_to_update = pd.read_csv(occ_codes_update_path, index_col=0, header=0, squeeze=True).to_dict()
    except Exception as ex:
        print(ex)
        print(f"Could not find the file:\n{occ_codes_update_path}\n")

    return occ_codes_to_update

def prepare_base_data(database, raw_table, processed_table, occ_codes_to_update):
    """
    Instead of modifying the data, we want to create a new table with the suffix _PROCESSED.
    So we will read in the ESR-ABSENCE-yyyy-mm_RAW table and use it to create ESR-ABSENCE-yyyy-mm_PROCESSED

    Step 1: Select everything in _RAW into the new table _PROCESSED
    Step 2: Run a loop to update all of the invalid occ codes
    """

    print(f"Populating the {processed_table} table\n")
    populate_table_query = f"""INSERT INTO [{processed_table}] SELECT * FROM [{raw_table}]"""
    execute_sql(database, populate_table_query)

    print(f"Updating the occ codes in the {processed_table} table\n")
    for old_val, new_val in occ_codes_to_update.items():
        print(f"Setting code {old_val} to be {new_val}")
        update_query = f"""UPDATE [{processed_table}]
                            SET [Occupation Code] = '{new_val}'
                            WHERE [Occupation Code] = '{old_val}'
                        """
        execute_sql(database, update_query)
