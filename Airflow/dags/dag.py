
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
from sqlalchemy import create_engine
import pandas as pd
import multiprocessing

host = "postgres_storage"
database = "dcsv"
user = "maw"
password = "1234"
port = '5432'


engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')


def Get_DF(Day):
    DF_index=None
    
    try: 
        day_URL=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
        DF_day=pd.read_csv(day_URL)
        DF_day['Day']=Day
        condition=(DF_day.Country_Region=='United Kingdom')
        Selected_columns=['Day','Country_Region', 'Last_Update',
          'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
          'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
        DF_index=DF_day[condition][Selected_columns].reset_index(drop=True)
    except:
        pass
    return DF_index



def fetch_data(**context):
    List_of_days=[]
    for year in range(2020,2022):
        for month   in range(1,13):
            for day in range(1,32):
                month=int(month)
                if day <=9:
                    day=f'0{day}'
                if month <= 9 :
                    month=f'0{month}'
                List_of_days.append(f'{month}-{day}-{year}')

    
    lst_all_DFs=[]
  
    for Day in List_of_days:
        lst_all_DFs.append(Get_DF(Day))
    
    #ConvertList to DF 
    DF_all = pd.concat(lst_all_DFs).reset_index(drop=True)
    DF_all.to_csv('/home/shared/origin_data.csv')
    
def minMax_scale(**context):
    DF_UK=pd.read_csv('/home/shared/origin_data.csv')
    Selected_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
    DF_UK_2 = DF_UK[Selected_Columns]


    from sklearn.preprocessing import MinMaxScaler

    min_max_scaler = MinMaxScaler()
    DF_UK_3 = pd.DataFrame(min_max_scaler.fit_transform(DF_UK_2),columns=Selected_Columns)
    DF_UK_3.index=DF_UK_2.index
    DF_UK_3['Day']=DF_UK.Day
    DF_UK_3.to_csv('/home/shared/Scaled_data.csv')


def push_data(**context):
    DF_UK=pd.read_csv('/home/shared/origin_data.csv')
    DF_UK_3=pd.read_csv('/home/shared/Scaled_data.csv')
    DF_UK.to_sql('nonscaled_data', engine,if_exists='replace',index=False)
    DF_UK_3.to_sql('scaled_data', engine,if_exists='replace',index=False)
    


def install_tools():

    try:
        import psycopg2
    except:
        subprocess.check_call(['pip', 'install', 'psycopg2-binary'])
        import psycopg2

    try:
        from sqlalchemy import create_engine
    except:
        subprocess.check_call(['pip', 'install', 'sqlalchemy'])
        from sqlalchemy import create_engine
        
    try:
        import pandas as pd
    except:
        subprocess.check_call(['pip', 'install', 'pandas'])
        import pandas as pd
        
    try:
        import matplotlib 
    except:
        subprocess.check_call(['pip', 'install', 'matplotlib'])
        import matplotlib
        
    try:
        import sklearn 
    except:
        subprocess.check_call(['pip', 'install', 'sklearn'])
        import sklearn        



def png(**context):
    import matplotlib.pyplot as plt 
    import matplotlib
    font = {'weight' : 'bold',
        'size'   : 18}
    matplotlib.rc('font', **font)
    plt.figure(figsize=(12,8))
    DF_UK=pd.read_csv('/home/shared/origin_data.csv')
    Selected_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
    DF_UK_u = DF_UK.copy()
    DF_UK_u[Selected_Columns].plot(figsize=(20,10))
    plt.savefig('/home/output/UK_scoring_report.png')

with DAG("JHC-UK", start_date=datetime(2021, 1, 1),
         schedule_interval="0 3 * * *", catchup=False) as dag: #to run it everyday at 3 PM
    installtools = PythonOperator(
        task_id="installtools",
        python_callable=install_tools,
        provide_context=True
    )
    
    fetchdata = PythonOperator(
        task_id="fetch",
        python_callable=fetch_data,
        provide_context=True
    )

    minMax = PythonOperator(
        task_id="minMax_data",
        python_callable=minMax_scale,
        provide_context=True
    )
    
    pushToPostgress = PythonOperator(
        task_id="pushToPostgress",
        python_callable=push_data,
        provide_context=True
    )

    pngReport = PythonOperator(
        task_id="png_Report",
        python_callable=png,
        provide_context=True
    )
    installtools >> fetchdata >> minMax  >> pushToPostgress >>  pngReport