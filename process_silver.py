import pandas as pd
import config
from datetime import datetime
from dateutil.relativedelta import relativedelta

MONTHS_IN_YEAR = 12

def format_start_date(employees_dataframe):
    '''
        Formats start_data from the given dataframe and
        changes format to config.NEW_DATE_FORMAT.
    '''
    employees_dataframe['start_date'] = pd.to_datetime(employees_dataframe['start_date'])
    employees_dataframe['start_date'] = employees_dataframe['start_date'].dt.strftime(config.NEW_DATE_FORMAT)
    return employees_dataframe

def fill_tenure(date):
    '''
        Calculates and fills the tenure
        for the date.
    '''
    todays_date = datetime.today()
    date = datetime.strptime(date, config.NEW_DATE_FORMAT)
    date_difference = relativedelta(todays_date, date)
    return date_difference.years*MONTHS_IN_YEAR + date_difference.months

def enrich_salaries(employee_id, deparments_dataframe, employees_dataframe):
    ''''
        In this function we obtain first name, last name,
        postion, tenure, department, location and manager
        based on the given employee_id.
    '''
    employee_query = employees_dataframe['employee_id'] == employee_id
    employee_columns = ['first_name', 'last_name', 'position', 'tenure_in_months', 'department']
    first_name, last_name, position, tenure_in_months, department_name = employees_dataframe[employee_query][employee_columns].iloc[0]
    
    department_query = deparments_dataframe['department'] == department_name
    department_columns = ['location', 'manager']
    location, manager = deparments_dataframe[department_query][department_columns].iloc[0]
    
    return first_name, last_name, position, tenure_in_months, department_name, location, manager

def silver_processor(salaries_dataframe, departments_dataframe, employees_dataframe, verbose, 
              save_path_salaries, save_path_employees):
    '''
        Uses all functions from the file to
        write adequate csv files in silver folder.
    '''
    employees_dataframe = format_start_date(employees_dataframe)
    employees_dataframe['tenure_in_months'] = employees_dataframe['start_date'].apply(fill_tenure)

    enrichment_columns = ['first_name', 'last_name', 'position', 'tenure_in_months', 'department', 'location', 'manager']

    salaries_dataframe[enrichment_columns] = None
    salaries_dataframe[enrichment_columns] = salaries_dataframe['employee_id'].apply(enrich_salaries, 
                                                                                     args=(departments_dataframe, 
                                                                                           employees_dataframe)).apply(pd.Series)
    
    salaries_dataframe.to_parquet(save_path_salaries)
    employees_dataframe.to_parquet(save_path_employees)
    if verbose:
        print (f'''Salaries and employees dataframes are written to 
               {save_path_salaries} and {save_path_employees}, respectively.''')