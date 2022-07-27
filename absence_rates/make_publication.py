import timeit
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import write_excel
import write_reason_absence_excel
from data_connections import get_df_from_sql
from preprocessing import prepare_base_data, read_occ_code_update_mappings
from reason_and_staff import sql_query_reason_staff, create_reason_absence_breakdowns
from helpers import (get_config, get_excel_template_dir, 
                    configure_logging)
from absence_rates import (query_base_data, create_absence_rates_breakdowns, 
                        create_org_absence_breakdowns)
from benchmarking_tool import (sql_query_benchmark_data, sql_latest_org_name, 
                            agg_benchmarking_orgs, create_benchmarking_tool)
from covid_table import (sql_query_covid_data, create_covid_breakdowns, 
                    create_covid_orgs_breakdowns, covid_joined_table, covid_final_table)


def main():
    """
    Creates a function which pulls in all the fields needed to make the publication as well as defining where the outputs should be saved.
    Allows a series of dataframes to be created from this which populate the NHS Sickness Absence publication tables.
    """
    config = get_config()

    database = config['database']
    staff_table = config['staff_table']
    ref_table = config['ref_table']
    mds_table = config['mds_table']
    ref_payscale = config['ref_payscale']
    org_master = config['org_master']
    latest_org_name = config['latest_org_name']
    month_date = config['month_date']
    start_date = config['start_date']
    end_date = config['end_date']
    staff_in_post = config['staff_in_post']

    output_dir = Path(config['output_dir'])
    log_dir = Path(config['log_dir'])
    template_dir = get_excel_template_dir()

    configure_logging(log_dir)
    logger = logging.getLogger(__name__)
    logger.info(f"Logging the config settings:\n\n\t{config}\n")
    logger.info(f"Starting run at:\t{datetime.now().time()}")
    
    # Sickness Absence data
    base_data_query = query_base_data(database, staff_table, org_master, ref_payscale, ref_table, start_date, end_date)
    base_absence_data = get_df_from_sql(database, base_data_query)
    
    # Sickness Absence by reason and staff group data
    reason_staff_query = sql_query_reason_staff(database, mds_table, staff_in_post, org_master, ref_table, start_date, end_date)
    base_reason_staff_data = get_df_from_sql(database, reason_staff_query)
    
    # Benchmarking sickness absence data
    base_bench_data_query = sql_query_benchmark_data(database, staff_table, org_master, ref_table, start_date, end_date)
    base_benchmarking_data = get_df_from_sql(database, base_bench_data_query)
    base_last_orgs_data_query = sql_latest_org_name(database, latest_org_name)
    base_latest_orgs_data = get_df_from_sql(database, base_last_orgs_data_query)
    
    # COVID-19 related sickness absence data
    base_covid_data_query = sql_query_covid_data(database, mds_table, staff_in_post, org_master, ref_table, start_date, end_date)
    base_covid_data = get_df_from_sql(database, base_covid_data_query)
    
    #### CSV and Excel production ####
    
    # Sickness Absence CSVs
    csv_1_path = output_dir / f"csv_absence_excel_production_{start_date}.csv" # used to create the Absence excel tables
    csv_1_outputs = create_absence_rates_breakdowns(base_absence_data)
    csv_1_outputs.to_csv(csv_1_path, index=False)

    csv_2_path = output_dir / f"csv_absence_rates_{start_date}.csv"
    csv_2_outputs = create_org_absence_breakdowns(base_absence_data, month_date)
    csv_2_outputs.to_csv(csv_2_path, index=False)
    
    # Sickness Absence by reason and staff group CSV
    reason_absence_path = output_dir / f"reason_absence_{start_date}.csv"
    reason_absence_outputs = create_reason_absence_breakdowns(base_reason_staff_data, month_date)
    reason_absence_outputs.to_csv(reason_absence_path, index=False)
    
    # Benchmarking sickness absence CSV
    benchmarking_csv_path = output_dir / f"benchmarking_csv_{start_date}.csv"
    benchmarking_inter_data = agg_benchmarking_orgs(base_benchmarking_data)
    benchmarking_csv_outputs = create_benchmarking_tool(benchmarking_inter_data, base_latest_orgs_data, month_date)
    benchmarking_csv_outputs.to_csv(benchmarking_csv_path, index=False)

    # COVID-19 related sickness absence CSV
    covid_path = output_dir / f"covid_{start_date}.csv"
    covid_inter_data = create_covid_breakdowns(base_covid_data)
    covid_inter_org_data = create_covid_orgs_breakdowns(base_covid_data)
    covid_joined_orgs = covid_joined_table(covid_inter_org_data, base_latest_orgs_data)
    covid_outputs = covid_final_table(covid_inter_data, covid_joined_orgs, month_date)
    covid_outputs.to_csv(covid_path, index=False)
    
    # To produce Sickness Absence Monthly Tables
    excel_template = template_dir / 'sickness_absence_monthly_template.xlsx'
    excel_output = output_dir / f"NHS_sickness_absence_rates_{start_date}.xlsx"
    table_1_data = write_excel.prepare_table_1(csv_1_path)
    table_2_data = write_excel.prepare_table_2(csv_1_path)
    table_3_data = write_excel.prepare_table_3(csv_1_path)

    # Sickness Absence Monthly Table 2 tag data groups
    table_2_1_data = write_excel.prepare_table_2_1(table_2_data)
    table_2_2_data = write_excel.prepare_table_2_2(table_2_data)
    table_2_3_data = write_excel.prepare_table_2_3(table_2_data)
    table_2_4_data = write_excel.prepare_table_2_4(table_2_data)
    table_2_5_data = write_excel.prepare_table_2_5(table_2_data)
    table_2_6_data = write_excel.prepare_table_2_6(table_2_data)
    table_2_7_data = write_excel.prepare_table_2_7(table_2_data)

    # Prepare the list of excel tables you want to write to
    tables = [
        {"sheet_name": "Table 1", "tag": "tag_table1", "data": table_1_data},
        {"sheet_name": "Table 2", "tag": "tag_table2_1", "data": table_2_1_data},
        {"sheet_name": "Table 2", "tag": "tag_table2_2", "data": table_2_2_data},
        {"sheet_name": "Table 2", "tag": "tag_table2_3", "data": table_2_3_data},
        {"sheet_name": "Table 2", "tag": "tag_table2_4", "data": table_2_4_data},
        {"sheet_name": "Table 2", "tag": "tag_table2_5", "data": table_2_5_data},
        {"sheet_name": "Table 2", "tag": "tag_table2_6", "data": table_2_6_data},
        {"sheet_name": "Table 2", "tag": "tag_table2_7", "data": table_2_7_data},
        {"sheet_name": "Table 3", "tag": "tag_table_3", "data": table_3_data}
    ]
    write_excel.write_tables_to_excel(tables, excel_template, excel_output)
           
    # To produce Sickness Absence by reason and staff Group tables
    reason_excel_template = template_dir / 'NHS_Sickness_by_reason_and_staff_template.xlsx'
    reason_excel_output = output_dir / f"NHS_Sickness_by_reason_and_staff_{start_date}.xlsx"
    reason_table_1_data = write_reason_absence_excel.prepare_reason_table_1(reason_absence_path)
    reason_table_2_data = write_reason_absence_excel.prepare_reason_table_2(reason_absence_path)

    # Sickness Absence by reason and staff group Table 2 tag data groups
    reason_table_1_1_data = write_reason_absence_excel.prepare_reason_table_1_1(reason_table_1_data)
    reason_table_1_2_data = write_reason_absence_excel.prepare_reason_table_1_2(reason_table_1_data)
    reason_table_1_3_a_data = write_reason_absence_excel.prepare_reason_table_1_3_a(reason_table_1_data)
    reason_table_1_3_b_data = write_reason_absence_excel.prepare_reason_table_1_3_b(reason_table_1_data)
    reason_table_1_3_c_data = write_reason_absence_excel.prepare_reason_table_1_3_c(reason_table_1_data)
    reason_table_1_3_d_data = write_reason_absence_excel.prepare_reason_table_1_3_d(reason_table_1_data)
    reason_table_1_3_e_data = write_reason_absence_excel.prepare_reason_table_1_3_e(reason_table_1_data)
    reason_table_1_3_f_data = write_reason_absence_excel.prepare_reason_table_1_3_f(reason_table_1_data)
    reason_table_1_3_g_data = write_reason_absence_excel.prepare_reason_table_1_3_g(reason_table_1_data)
    reason_table_1_3_h_data = write_reason_absence_excel.prepare_reason_table_1_3_h(reason_table_1_data)
    reason_table_1_3_i_data = write_reason_absence_excel.prepare_reason_table_1_3_i(reason_table_1_data)
    reason_table_1_3_j_data = write_reason_absence_excel.prepare_reason_table_1_3_j(reason_table_1_data)
    reason_table_1_3_k_data = write_reason_absence_excel.prepare_reason_table_1_3_k(reason_table_1_data)
    reason_table_1_4_a_data = write_reason_absence_excel.prepare_reason_table_1_4_a(reason_table_1_data)
    reason_table_1_4_b_data = write_reason_absence_excel.prepare_reason_table_1_4_b(reason_table_1_data)
    reason_table_1_4_c_data = write_reason_absence_excel.prepare_reason_table_1_4_c(reason_table_1_data)
    reason_table_1_4_d_data = write_reason_absence_excel.prepare_reason_table_1_4_d(reason_table_1_data)
    reason_table_1_5_a_data = write_reason_absence_excel.prepare_reason_table_1_5_a(reason_table_1_data)
    reason_table_1_5_b_data = write_reason_absence_excel.prepare_reason_table_1_5_b(reason_table_1_data)
    reason_table_1_5_c_data = write_reason_absence_excel.prepare_reason_table_1_5_c(reason_table_1_data)
    reason_table_1_5_d_data = write_reason_absence_excel.prepare_reason_table_1_5_d(reason_table_1_data)
    reason_table_1_6_a_data = write_reason_absence_excel.prepare_reason_table_1_6_a(reason_table_1_data)
    reason_table_1_6_b_data = write_reason_absence_excel.prepare_reason_table_1_6_b(reason_table_1_data)
    reason_table_1_6_c_data = write_reason_absence_excel.prepare_reason_table_1_6_c(reason_table_1_data)
    reason_table_1_6_d_data = write_reason_absence_excel.prepare_reason_table_1_6_d(reason_table_1_data)
    reason_table_1_6_e_data = write_reason_absence_excel.prepare_reason_table_1_6_e(reason_table_1_data)
    reason_table_1_7_data = write_reason_absence_excel.prepare_reason_table_1_7(reason_table_1_data)

    reason_table_2_1_data = write_reason_absence_excel.prepare_reason_table_2_1(reason_table_2_data)
    reason_table_2_2_data = write_reason_absence_excel.prepare_reason_table_2_2(reason_table_2_data)
    reason_table_2_3_a_data = write_reason_absence_excel.prepare_reason_table_2_3_a(reason_table_2_data)
    reason_table_2_3_b_data = write_reason_absence_excel.prepare_reason_table_2_3_b(reason_table_2_data)
    reason_table_2_3_c_data = write_reason_absence_excel.prepare_reason_table_2_3_c(reason_table_2_data)
    reason_table_2_3_d_data = write_reason_absence_excel.prepare_reason_table_2_3_d(reason_table_2_data)
    reason_table_2_3_e_data = write_reason_absence_excel.prepare_reason_table_2_3_e(reason_table_2_data)
    reason_table_2_3_f_data = write_reason_absence_excel.prepare_reason_table_2_3_f(reason_table_2_data)
    reason_table_2_3_g_data = write_reason_absence_excel.prepare_reason_table_2_3_g(reason_table_2_data)
    reason_table_2_3_h_data = write_reason_absence_excel.prepare_reason_table_2_3_h(reason_table_2_data)
    reason_table_2_3_i_data = write_reason_absence_excel.prepare_reason_table_2_3_i(reason_table_2_data)
    reason_table_2_3_j_data = write_reason_absence_excel.prepare_reason_table_2_3_j(reason_table_2_data)
    reason_table_2_3_k_data = write_reason_absence_excel.prepare_reason_table_2_3_k(reason_table_2_data)
    reason_table_2_4_a_data = write_reason_absence_excel.prepare_reason_table_2_4_a(reason_table_2_data)
    reason_table_2_4_b_data = write_reason_absence_excel.prepare_reason_table_2_4_b(reason_table_2_data)
    reason_table_2_4_c_data = write_reason_absence_excel.prepare_reason_table_2_4_c(reason_table_2_data)
    reason_table_2_4_d_data = write_reason_absence_excel.prepare_reason_table_2_4_d(reason_table_2_data)
    reason_table_2_5_a_data = write_reason_absence_excel.prepare_reason_table_2_5_a(reason_table_2_data)
    reason_table_2_5_b_data = write_reason_absence_excel.prepare_reason_table_2_5_b(reason_table_2_data)
    reason_table_2_5_c_data = write_reason_absence_excel.prepare_reason_table_2_5_c(reason_table_2_data)
    reason_table_2_5_d_data = write_reason_absence_excel.prepare_reason_table_2_5_d(reason_table_2_data)
    reason_table_2_6_a_data = write_reason_absence_excel.prepare_reason_table_2_6_a(reason_table_2_data)
    reason_table_2_6_b_data = write_reason_absence_excel.prepare_reason_table_2_6_b(reason_table_2_data)
    reason_table_2_6_c_data = write_reason_absence_excel.prepare_reason_table_2_6_c(reason_table_2_data)
    reason_table_2_6_d_data = write_reason_absence_excel.prepare_reason_table_2_6_d(reason_table_2_data)
    reason_table_2_6_e_data = write_reason_absence_excel.prepare_reason_table_2_6_e(reason_table_2_data)
    reason_table_2_7_data = write_reason_absence_excel.prepare_reason_table_2_7(reason_table_2_data)

    # Prepare the list of excel tables you want to write to
    reason_tables = [
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_1", "data": reason_table_1_1_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_2", "data": reason_table_1_2_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_a", "data": reason_table_1_3_a_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_b", "data": reason_table_1_3_b_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_c", "data": reason_table_1_3_c_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_d", "data": reason_table_1_3_d_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_e", "data": reason_table_1_3_e_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_f", "data": reason_table_1_3_f_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_g", "data": reason_table_1_3_g_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_h", "data": reason_table_1_3_h_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_i", "data": reason_table_1_3_i_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_j", "data": reason_table_1_3_j_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_3_k", "data": reason_table_1_3_k_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_4_a", "data": reason_table_1_4_a_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_4_b", "data": reason_table_1_4_b_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_4_c", "data": reason_table_1_4_c_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_4_d", "data": reason_table_1_4_d_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_5_a", "data": reason_table_1_5_a_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_5_b", "data": reason_table_1_5_b_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_5_c", "data": reason_table_1_5_c_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_5_d", "data": reason_table_1_5_d_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_6_a", "data": reason_table_1_6_a_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_6_b", "data": reason_table_1_6_b_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_6_c", "data": reason_table_1_6_c_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_6_d", "data": reason_table_1_6_d_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_6_e", "data": reason_table_1_6_e_data},
        {"sheet_name": "Table 1 (%)", "tag": "tag_table1_7", "data": reason_table_1_7_data},

        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_1", "data": reason_table_2_1_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_2", "data": reason_table_2_2_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_a", "data": reason_table_2_3_a_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_b", "data": reason_table_2_3_b_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_c", "data": reason_table_2_3_c_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_d", "data": reason_table_2_3_d_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_e", "data": reason_table_2_3_e_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_f", "data": reason_table_2_3_f_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_g", "data": reason_table_2_3_g_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_h", "data": reason_table_2_3_h_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_i", "data": reason_table_2_3_i_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_j", "data": reason_table_2_3_j_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_3_k", "data": reason_table_2_3_k_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_4_a", "data": reason_table_2_4_a_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_4_b", "data": reason_table_2_4_b_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_4_c", "data": reason_table_2_4_c_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_4_d", "data": reason_table_2_4_d_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_5_a", "data": reason_table_2_5_a_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_5_b", "data": reason_table_2_5_b_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_5_c", "data": reason_table_2_5_c_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_5_d", "data": reason_table_2_5_d_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_6_a", "data": reason_table_2_6_a_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_6_b", "data": reason_table_2_6_b_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_6_c", "data": reason_table_2_6_c_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_6_d", "data": reason_table_2_6_d_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_6_e", "data": reason_table_2_6_e_data},
        {"sheet_name": "Table 2 (Count)", "tag": "tag_table2_7", "data": reason_table_2_7_data},
    ]
    write_excel.write_tables_to_excel(reason_tables, reason_excel_template, reason_excel_output)   

if __name__ == '__main__':
    print(f"Running publication")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    print(f"Running time of create_publication: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.")
