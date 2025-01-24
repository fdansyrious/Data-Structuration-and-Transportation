from datetime import date, timedelta
from env import DATABASE_FOLDER, DB_NAME, REPORTS_FOLDER
import pandas as pd
import sqlite3
import logging

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

default_global_args = {
    "depends_on_past": False,
    # "email": ["fd@fakeemail.com"],
    # "email_on_failure": False,
    # "email_on_try": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'on_skipped_callback': another_function, #or list of functions
    # 'trigger_rule': 'all_success'
}


def query_data_from_db(query: str) -> pd.DataFrame:
    with sqlite3.connect(DATABASE_FOLDER+"/"+DB_NAME) as connection:
        try : 
            df = pd.read_sql_query(query, con=connection)
            return df
        except Exception as e:
            logger.debug(f"Error encountered while reading data.\n Failed to excute query {query}\n {e}")
    connection.close()
        


@dag(dag_id="school_mng_dag",
    default_args=default_global_args, 
    description="DAG for school indicators reporting",
    schedule_interval=timedelta(weeks=2), 
    start_date=days_ago(1),
    catchup=False,
    tags=["school_reporting"]
    )
def automate_reporting():
    
    #Data Extraction Task    
    @task(task_id="extract_consolidated_data")
    def data_extraction_task()-> pd.DataFrame:
        query = """SELECT t.TeacherID , t.TeacherName , c.CourseName, s.StudentName, g.*
                    FROM grades as g
                    INNER JOIN courses as c
                    ON g.CourseID = c.CourseID
                    INNER JOIN teachers as t 
                    ON t.TeacherID = c.TeacherID
                    INNER JOIN students as s
                    ON g.StudentID = s.StudentID"""
        df_consolidated = query_data_from_db(query)
        return df_consolidated
        
    @task(task_id="build_teachers_reporting")
    def teachers_reporting_task(df_consolidated:pd.DataFrame)-> pd.DataFrame:
        df_teachers = df_consolidated.groupby(["TeacherID", "TeacherName", "CourseName"]).agg({"Score": ["min", "max", "mean"]})
        df_teachers.columns = ["min_score", "max_score", "avg_score"]
        df_teachers = df_teachers.reset_index()
        return df_teachers
        
    @task(task_id="build_students_reporting")
    def students_reporting_task(df_consolidated:pd.DataFrame)-> pd.DataFrame:
        df_students = df_consolidated.groupby(["StudentID", "StudentName", "CourseName"]).agg({"Score": "mean"})
        df_students.columns = ["avg_score"]
        df_students = df_students.reset_index()
        return df_students
    
    @task(task_id="save_reporting")
    def saving_reporting_task(df_teachers:pd.DataFrame, df_students:pd.DataFrame)-> None:
        dt = date.today().strftime("%d%m%Y")
        
        logger.info("Saving Teachers Reporting file")
        df_teachers.to_csv(REPORTS_FOLDER+f"/{dt}_teachers_report.csv", index=False)
        
        logger.info("Saving Students Reporting file")
        df_students.to_csv(REPORTS_FOLDER+f"/{dt}_students_report.csv", index=False)
        
    df_consolidated = data_extraction_task()
    df_teachers = teachers_reporting_task(df_consolidated)
    df_students = students_reporting_task(df_consolidated)
    saving_reporting_task(df_teachers, df_students)
    
automate_reporting()