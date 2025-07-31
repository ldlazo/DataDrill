import pandas as pd
import matplotlib.pyplot as plt
import config
import os

def check_images_reports_dir():
    if not os.path.exists(config.IMAGES_REPORTS):
        os.makedirs(config.IMAGES_REPORTS)

def calculate_average_gross_salaries_per_department(salaries_enriched, plot):
    '''
        From the salaries_enriched dataframe takes last month and year
        and calculates average gross salaries per deparment.
    '''
    temp_df = salaries_enriched.sort_values(by=['year', 'month'])
    last_year, last_month = temp_df[['year', 'month']].iloc[-1]

    salaries_enriched = salaries_enriched[(salaries_enriched['year'] == last_year) 
                                        & (salaries_enriched['month'] == last_month)]

    average_salaries_per_department = salaries_enriched.groupby('department')['gross_salary'].mean()
    average_salaries_per_department_dataframe = pd.DataFrame({'average_gross_salary_per_department':average_salaries_per_department})
    if plot:
        average_salaries_per_department_dataframe.plot(kind='bar')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'{config.IMAGES_REPORTS}\\{config.AVERAGE_SALARY_PER_DEPARTMENT_IMG_PATH}')
    return average_salaries_per_department_dataframe

def calculate_employees_number_per_location(employees_dataframe, departments_dataframe, plot):
    '''
        Merges employees and departments dataframes on department 
        and calculates employees number per location.
    '''
    merged_dataframe = pd.merge(employees_dataframe, departments_dataframe, on='department', how='inner')
    employees_per_location = merged_dataframe.groupby('location')['employee_id'].count()
    employees_per_location_dataframe = pd.DataFrame({'employees_per_location':employees_per_location})
    if plot:
        employees_per_location_dataframe.plot(kind='bar')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'{config.IMAGES_REPORTS}\\{config.NUMBER_OF_EMPLOYEES_PER_LOCATION_IMG_PATH}')

    return employees_per_location_dataframe

def calculate_max_average_tenure_department(salaries_enriched):
    '''
        Uses salaries enriched dataframe where we have more
        data about the employees and gives us the name of 
        the deparment with the max average tenure.
    '''
    salaries_enriched_shortened = salaries_enriched[['employee_id', 'department', 'tenure_in_months']].drop_duplicates()
    max_average_tenure_department_name = salaries_enriched_shortened.groupby('department')['tenure_in_months'].mean().idxmax()
    max_average_tenure_department_dataframe = pd.DataFrame({'max_average_tenure_department': max_average_tenure_department_name}, 
                                                           index=[0])

    return max_average_tenure_department_dataframe

def gold_processor(salaries_enriched, employees_dataframe, departments_dataframe, verbose, save_path, plot):

    if plot:
        check_images_reports_dir()

    average_salaries_per_department_dataframe = calculate_average_gross_salaries_per_department(salaries_enriched, plot)
    employees_per_location_dataframe = calculate_employees_number_per_location(employees_dataframe, departments_dataframe, plot)
    max_average_tenure_department_dataframe = calculate_max_average_tenure_department(salaries_enriched)

    average_salaries_per_department_dict = average_salaries_per_department_dataframe.to_dict()
    employees_per_location_dict = employees_per_location_dataframe.to_dict()
    max_average_tenure_department_dict = max_average_tenure_department_dataframe.to_dict()

    summary_dict = {**average_salaries_per_department_dict, 
                    **employees_per_location_dict, 
                    **max_average_tenure_department_dict}
    
    summary_dataframe = pd.DataFrame(summary_dict)
    summary_dataframe.to_csv(save_path)
    if verbose:
        print(f'''Average salaries per department, number of employees per location are calculated 
              and the department with the maximum average tenure are obtained and written to {save_path}.''')