from datetime import datetime, timedelta
import math
import requests

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

default_args = {
    'owner': 'shazy',
    'retry': 5,
    'retry_delay': timedelta(minutes=3)
}
faker_ip = Variable.get('FAKER_IP')


@dag(dag_id='Dag_for_FakerAPI',
     default_args=default_args,
     start_date=datetime(2024, 1, 17, 2),
     schedule_interval='@daily')
def data_from_api():
    @task()
    def get_weather(ti):
        api_url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 55.7522,
            "longitude": 37.6156,
            "daily": ["temperature_2m_max", "temperature_2m_min"],
            "timezone": "Europe/Moscow"
        }

        data = requests.get(api_url, params=params).json()
        tomorrow_temp_max = data['daily']['temperature_2m_max'][0]
        tomorrow_temp_min = data['daily']['temperature_2m_min'][0]
        avg_temp = (tomorrow_temp_min + tomorrow_temp_max) / 2
        result_temperature = math.ceil(abs(avg_temp)) if avg_temp != 0 else 1

        ti.xcom_push(key='temp', value=result_temperature)

    @task()
    def extract_data_from_main_db(ti):
        max_buyers_id_exec = PostgresOperator(
            task_id='execute_max_buyers_id',
            postgres_conn_id='main_data',
            sql='SELECT MAX(id) FROM buyers'
        )

        max_products_id_exec = PostgresOperator(
            task_id='execute_max_prod_id',
            postgres_conn_id='main_data',
            sql='SELECT MAX(id) FROM products'
        )

        max_buyers_id = max_buyers_id_exec.execute(context=None)[0]
        max_prod_id = max_products_id_exec.execute(context=None)[0]
        print(f'Max buyers ID: {max_buyers_id}\nMax products id: {max_prod_id}')

        ti.xcom_push(key='max_buyers_id', value=max_buyers_id[0])
        ti.xcom_push(key='max_products_id', value=max_prod_id[0])

    @task()
    def orders_generation(ratio: int, max_buyers_id: int, max_products_id: int, ti):
        day_number = datetime.now().day
        daily_coefficient = 0
        temperature_coefficient = ratio * 0.80646

        if day_number < 12:
            daily_coefficient = abs(1 / (day_number - 12)) / 1 - 0.098
        elif day_number == 12:
            daily_coefficient = 1
        elif day_number > 12:
            daily_coefficient = 1 - (day_number - 12) / 20

        data = {
            'max_users_id': max_buyers_id,
            'max_products_id': max_products_id,
            'max_items': math.ceil(daily_coefficient * temperature_coefficient)
        }

        datas = []
        api_url = f'http://{faker_ip}/get_order'

        for _ in range(math.ceil(ratio / 2)):
            response = requests.post(api_url, json=data)
            datas.append(response.json())

        ti.xcom_push(key='data', value=datas)
        ti.xcom_push(key='max_items', value=math.ceil(daily_coefficient * temperature_coefficient))

    @task
    def new_buyers_generation(ratio: int, max_buyers_id: int, max_products_id: int, max_items: int, ti):
        new_buyers_count = math.ceil(ratio / 4)

        api_url = f'http://{faker_ip}/get_user'
        new_users = []

        for i in range(new_buyers_count):
            response = requests.get(api_url)
            new_users.append(response.json())

        print(new_users)
        ti.xcom_push(key='new_users', value=new_users)

        api_url = f'http://{faker_ip}/get_user_order'
        new_users_orders = []

        for new_user_id in range(max_buyers_id + 1, max_buyers_id + new_buyers_count + 1):
            data = {
                'user_id': new_user_id,
                'max_products_id': max_products_id,
                'max_items': max_items,
            }
            response = requests.post(api_url, json=data)
            new_users_orders.append(response.json())

        print(new_users_orders)
        ti.xcom_push(key='new_users_orders', value=new_users_orders)

    @task()
    def load_orders(orders):
        for order in orders:
            insert_order = PostgresOperator(
                task_id='insert_order',
                postgres_conn_id='main_data',
                sql="""
                    INSERT INTO orders (buyers_id, item_ids, time)
                    VALUES (%(buyers_id)s, %(item_ids)s, %(time)s)
                    """,
                parameters={
                    'buyers_id': order['buyer_id'],
                    'item_ids': order['item_ids'],
                    'time': order['time'],
                }
            )

            insert_order.execute(context=None)

    @task()
    def load_new_buyers(new_buyers, buyers_orders):
        for buyer, order in zip(new_buyers, buyers_orders):
            insert_buyer = PostgresOperator(
                task_id='insert_buyer_into_buyers',
                postgres_conn_id='main_data',
                sql="""
                    INSERT INTO buyers (geo_lon, geo_lat, place, region)
                    VALUES (%(longitude)s, %(latitude)s, %(place)s, %(region)s)
                    """,
                parameters={
                    'longitude': buyer['longitude'],
                    'latitude': buyer['latitude'],
                    'place': buyer['place'],
                    'region': buyer['region'],
                }
            )
            insert_buyers_order = PostgresOperator(
                task_id='insert_buyers_order_into_orders',
                postgres_conn_id='main_data',
                sql="""
                    INSERT INTO orders (buyers_id, item_ids, time)
                    VALUES (%(buyers_id)s, %(item_ids)s, %(time)s)
                    """,
                parameters={
                    'buyers_id': order['buyer_id'],
                    'item_ids': order['item_ids'],
                    'time': order['time'],
                }
            )

            insert_buyer.execute(context=None)
            insert_buyers_order.execute(context=None)

    extracted_data = extract_data_from_main_db()
    get_temperature = get_weather()
    new_orders_gen = orders_generation(
        ratio=get_temperature['temp'],
        max_buyers_id=extracted_data['max_buyers_id'],
        max_products_id=extracted_data['max_products_id']
    )
    new_buyers_gen = new_buyers_generation(
        ratio=get_temperature['temp'],
        max_buyers_id=extracted_data['max_buyers_id'],
        max_products_id=extracted_data['max_products_id'],
        max_items=new_orders_gen['max_items']
    )
    load_orders = load_orders(new_orders_gen['data'])
    load_new_buyers = load_new_buyers(
        new_buyers=new_buyers_gen['new_users'],
        buyers_orders=new_buyers_gen['new_users_orders']
    )


greet_dag = data_from_api()
