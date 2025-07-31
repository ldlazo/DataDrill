import pandas as pd
import config
import argparse
from process_bronze import validate_departments, validate_employees, validate_salaries
from process_silver import silver_processor
from process_gold import gold_processor

def parse_opt():

    parser = argparse.ArgumentParser()
    parser.add_argument("-verbose", 
                        type=bool, 
                        default=False, 
                        help="Boolean value determines whether you want prints or not."
                        )
    parser.add_argument("-valid_department_path", 
                        type=str, 
                        default=f'{config.BRONZE_RESULTS}\\{config.DEPARTMENTS_PARQUET_NAME}', 
                        help="Save path for validated departments."
                        )
    parser.add_argument("-valid_employees_path", 
                        type=str, 
                        default=f'{config.BRONZE_RESULTS}\\{config.EMPLOYEES_PARQUET_NAME}', 
                        help="Save path for validated employees."
                        )
    parser.add_argument("-valid_salaries_path", 
                        type=str, 
                        default=f'{config.BRONZE_RESULTS}\\{config.SALARIES_PARQUET_NAME}', 
                        help="Save path for validated salaries."
                        )
    parser.add_argument("-silver_salaries_path", 
                        type=str, 
                        default=f'{config.SILVER_RESULTS}\\{config.SALARIES_PARQUET_NAME}', 
                        help="Save path for extended salaries data."
                        )
    parser.add_argument("-silver_employees_path", 
                        type=str, 
                        default=f'{config.SILVER_RESULTS}\\{config.EMPLOYEES_PARQUET_NAME}', 
                        help="Save path for updated employees data."
                        )
    parser.add_argument("-gold_results_path", 
                        type=str, 
                        default=f'{config.GOLD_RESULTS}\\{config.GOLD_SUMMARY_REPORT}',
                        help="Save path for gold results."
                        )
    parser.add_argument("-plot", 
                        type=bool, 
                        default=False,
                        help="Whether to plot some summary reports or not."
                        )
    opt = parser.parse_args()
    return opt
    
def load_dataframes():
    departments_dataframe = pd.read_csv(config.DEPARTMENTS_CSV_PATH)
    employees_dataframe = pd.read_csv(config.EMPLOYEES_CSV_PATH)
    salaries_dataframe = pd.read_csv(config.SALARIES_CSV_PATH)

    return departments_dataframe, employees_dataframe, salaries_dataframe

def main(opt):
    departments_dataframe, employees_dataframe, salaries_dataframe = load_dataframes()
    #validate departments, employees and salaries data
    validate_departments(departments_dataframe=departments_dataframe, 
                         verbose=opt.verbose,save_path=opt.valid_department_path)
    validate_employees(employees_dataframe=employees_dataframe, 
                       departments_dataframe=departments_dataframe, 
                       verbose=opt.verbose, save_path=opt.valid_employees_path)
    validate_salaries(salaries_dataframe=salaries_dataframe, 
                      employees_dataframe=employees_dataframe,
                      verbose=opt.verbose, save_path=opt.valid_salaries_path)
    
    #process update employees data and enrich salaries data
    silver_processor(salaries_dataframe=salaries_dataframe, 
            departments_dataframe=departments_dataframe, 
            employees_dataframe=employees_dataframe,
            verbose=opt.verbose,
            save_path_employees=opt.silver_employees_path, 
            save_path_salaries=opt.silver_salaries_path)

    # gold part
    salaries_enriched = pd.read_parquet(opt.silver_salaries_path)
    gold_processor(salaries_enriched=salaries_enriched, 
                   employees_dataframe=employees_dataframe, 
                   departments_dataframe=departments_dataframe,
                   verbose=opt.verbose,
                   save_path=opt.gold_results_path,
                   plot=opt.plot)

if __name__ == "__main__":
    opt = parse_opt()
    main(opt)