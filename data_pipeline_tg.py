from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(hours=1),
}

dag = DAG(
    'data_pipeline_tg',
    default_args=default_args,
    description='Загрузка данных через Telegram и дальнейшая обработка',
    #schedule_interval=timedelta(minutes=5),
    start_date=days_ago(1),
)

t1 = BashOperator(
    task_id='Loading file from Telegram',
    bash_command='python3 /root/Bank-main/telegramBot.py',
    dag=dag,
)

t2 = BashOperator(
    task_id='Parsing retail turnover data 2009',
    bash_command='python3 /root/Bank-main/095_106_parser.py',
    dag=dag,
)

t3 = BashOperator(
    task_id='Parsing retail turnover food data 2009',
    bash_command='python3 /root/Bank-main/107_118_parser.py',
    dag=dag,
)

t4 = BashOperator(
    task_id='Parsing retail turnover non food data 2009',
    bash_command='python3 /root/Bank-main/119_130_parser.py',
    dag=dag,
)

t5 = BashOperator(
    task_id='Parsing retail turnover data',
    bash_command='python3 /root/Bank-main/05_01_parser.py',
    dag=dag,
)

t6 = BashOperator(
    task_id='Parsing retail turnover food data',
    bash_command='python3 /root/Bank-main/05_02_parser.py',
    dag=dag,
)

t7 = BashOperator(
    task_id='Parsing retail turnover non food data',
    bash_command='python3 /root/Bank-main/05_03_parser.py',
    dag=dag,
)

t8 = BashOperator(
    task_id='Deleting a file',
    bash_command='python3 /root/Bank-main/deletingFile.py',
    dag=dag,
)

t9 = BashOperator(
    task_id='Data Quality',
    bash_command='python3 /root/Bank-main/dataValidation.py',
    dag=dag,
)

t10 = BashOperator(
    task_id='Telegram Notification',
    bash_command='python3 /root/Bank-main/tgNotification.py',
    dag=dag,
)

# t11 = BashOperator(
#     task_id='Recount mom',
#     bash_command='python3 /root/Bank-main/recount.py',
#     dag=dag,
# )
#
# t12 = BashOperator(
#     task_id='Sarima predict rtt',
#     bash_command='python3 /root/Bank-main/sarima_rtt.py',
#     dag=dag,
# )
#
# t13 = BashOperator(
#     task_id='Sarima predict rtt food',
#     bash_command='python3 /root/Bank-main/sarima_rtt_food.py',
#     dag=dag,
# )
#
# t14 = BashOperator(
#     task_id='Sarima predict rtt non food',
#     bash_command='python3 /root/Bank-main/sarima_rtt_non_food.py',
#     dag=dag,
# )
#
# t15 = BashOperator(
#     task_id='LSTM predict rtt',
#     bash_command='python3 /root/Bank-main/lstm_rtt.py',
#     dag=dag,
# )
#
# t16 = BashOperator(
#     task_id='LSTM predict rtt food',
#     bash_command='python3 /root/Bank-main/lstm_rtt_food.py',
#     dag=dag,
# )
#
# t17 = BashOperator(
#     task_id='LSTM predict rtt non food',
#     bash_command='python3 /root/Bank-main/lstm_rtt_non_food.py',
#     dag=dag,
# )

t1 >> [t2, t3, t4, t5, t6, t7]
t8 << [t2, t3, t4, t5, t6, t7]
t8 >> t9
t9 >> t10
#t10 >> t11
#t11 >> [t12, t13, t14, t15, t16, t17]
