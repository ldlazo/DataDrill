import pandas as pd
import config
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from datetime import datetime
import re
from dateutil.relativedelta import relativedelta

def validate_departments(departments_dataframe, verbose, save_path):
    '''
        Validates departments data, removes invalid date from
        the dataframe and saves data at csv_path.
    '''
    departments_dataframe = departments_dataframe.dropna()
    departments_dataframe = departments_dataframe[departments_dataframe['location'].apply(is_location)]
    departments_dataframe = departments_dataframe[departments_dataframe['manager'].apply(is_manager_name_format_valid)]
    departments_dataframe.to_parquet(save_path)
    if verbose:
        print(f'Departments csv file is validated and written to {save_path}')

def validate_employees(employees_dataframe, departments_dataframe, verbose, save_path):
    '''
        Validates employees data, removes invalid date from
        the dataframe and saves data at csv_path.
    '''

    departments_list = get_list_of_departments(departments_dataframe)

    employees_dataframe = employees_dataframe[employees_dataframe['start_date'].apply(is_date_valid)]
    employees_dataframe = employees_dataframe[employees_dataframe['first_name'].apply(is_employee_name_valid)]
    employees_dataframe = employees_dataframe[employees_dataframe['last_name'].apply(is_employee_name_valid)]
    employees_dataframe = employees_dataframe[employees_dataframe['department'].apply(is_department,args=(departments_list, ))]

    employees_dataframe.to_parquet(save_path)
    if verbose:
        print(f'Employees csv file is validated and written to {save_path}')

def validate_salaries(salaries_dataframe, employees_dataframe, verbose, save_path):
    '''
        Validates salaries data, removes invalid date from
        the dataframe and saves data at csv_path.
    '''
    employees_list, employees_with_start_date = get_employee_list_and_start_date_for_each_empolyee(employees_dataframe)

    salaries_dataframe = salaries_dataframe[salaries_dataframe['gross_salary'] > 0]
    salaries_dataframe = salaries_dataframe.drop_duplicates()
    salaries_dataframe = salaries_dataframe[salaries_dataframe['employee_id'].apply(is_employee, args=(employees_list, ))]
    salaries_dataframe = salaries_dataframe[salaries_dataframe['year']<=datetime.today().year]
    salaries_dataframe = salaries_dataframe[(0 < salaries_dataframe['month']) & (salaries_dataframe['month'] <= 12)]
    salaries_dataframe = salaries_dataframe[salaries_dataframe[['employee_id', 'year', 'month']].apply(is_salary_possible,
                                                                                                       axis=1, 
                                                                                                       args=(employees_with_start_date,
                                                                                                             ))]
    
    salaries_dataframe.to_parquet(save_path)
    if verbose:
        print(f'Salaries csv file is validated and written to {save_path}')

def is_location(location_name):
    '''
        It checks whether the given location name is valid.
    '''
    try:
        geolocator = Nominatim(user_agent="location_checker")
        location = geolocator.geocode(location_name, exactly_one=True, timeout=10)
        return location is not None
    except GeocoderTimedOut:
        return False

def is_date_valid(date):
    '''
        It checks whether the given date is valid, 
        whether todays date isn't passed and wheather 
        the given date really exists.
    '''
    try:
        dt = datetime.strptime(date, config.DATE_FORMAT)
    except ValueError:
        return False

    return True if dt.date() <= datetime.today().date() else False

def is_manager_name_format_valid(name):
    '''
        Managers names are written in a specific format,
        we check whether the given name match the format.
    '''
    name_format = r'^[A-ZČĆŠĐŽ][a-zčćđšž]* [A-ZČĆŠĐŽ]\.$'
    return bool(re.match(name_format, name))

def is_employee_name_valid(name):
    ''''
        Employees names are written in a specific format,
        we check whether the given name match the format.
    '''
    name_format = r'^[A-ZČĆŠĐŽ][a-zčćđšž]*'
    return bool(re.match(name_format, name))

def is_department(department, departments_list):
    '''
        It checks whether the given department is one
        of the departments from the list.
    '''
    return True if department in departments_list else False

def is_employee(employee_id, employees_list):
    '''
        It checks whether the given employee is one
        of the employees from the list.
    '''
    return True if employee_id in employees_list else False

def get_list_of_departments(departments_dataframe):
    '''
        Returns list of all departments.
    '''
    departments_list = departments_dataframe['department'].to_list()
    return departments_list

def get_employee_list_and_start_date_for_each_empolyee(employees_dataframe):
    '''
        It returns list of all employees and dataframe
        where columns are employee_id, year and month when
        employee started to work.
    '''
    employees_list = employees_dataframe['employee_id'].to_list()
    employees_with_start_date = pd.DataFrame()
    employees_with_start_date['employee_id'] = employees_dataframe['employee_id']
    employees_with_start_date['start_date_year'] = employees_dataframe['start_date'].apply(get_start_year)
    employees_with_start_date['start_date_month'] = employees_dataframe['start_date'].apply(get_start_month)

    return employees_list, employees_with_start_date

def get_start_year(date):
    return int(date.split('-')[0])

def get_start_month(date):
    return int(date.split('-')[1])

def is_salary_possible(row, employees_with_start_date):
    '''
        We want to make sure that the specific employee
        could get salary for the specific month based
        on the start date.
    '''
    start_year = employees_with_start_date[employees_with_start_date['employee_id'] == row['employee_id']]['start_date_year'].values[0]
    start_month = employees_with_start_date[employees_with_start_date['employee_id'] == row['employee_id']]['start_date_month'].values[0]
    start_date = datetime(year=start_year, month=start_month, day=1)
    start_date = start_date + relativedelta(months=1)
    salary_date = datetime(year=row['year'], month=row['month'], day=1)

    return True if salary_date >= start_date else False