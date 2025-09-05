import requests
import json
import time
from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError

class AstronautsDataLoader:
    def __init__(self):
        self.api_url = "http://api.open-notify.org/astros.json"
        self.ch_client = Client(
            host='localhost',
            user='admin',  # или 'default' если используете пользователя по умолчанию
            password='password',
            port=9000
        )
        
    def get_astronauts_data(self):
        max_retries = 5
        base_delay = 1  # начальная задержка в секундах
        
        for attempt in range(max_retries):
            try:
                response = requests.get(self.api_url)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    delay = base_delay * (2 ** attempt)  # экспоненциальная задержка
                    print(f"Rate limit exceeded. Waiting {delay} seconds before retry {attempt + 1}/{max_retries}")
                    time.sleep(delay)
                else:
                    response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                print(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to get data after {max_retries} attempts") from e
                
                delay = base_delay * (2 ** attempt)
                time.sleep(delay)
    
    def insert_to_clickhouse(self, data):
        # Преобразуем данные в JSON строку
        json_data = json.dumps(data)
        
        # Вставляем данные в ClickHouse
        query = """
            INSERT INTO space_data.astronauts_raw (raw_json)
            VALUES (%(json_data)s)
        """
        
        try:
            self.ch_client.execute(query, {'json_data': json_data})
            print("Data successfully inserted into ClickHouse")
        except NetworkError as e:
            print(f"ClickHouse connection error: {str(e)}")
            raise
    
    def run(self):
        try:
            data = self.get_astronauts_data()
            if data:
                self.insert_to_clickhouse(data)
                print("Data loading completed successfully")
        except Exception as e:
            print(f"Failed to load data: {str(e)}")
            raise

if __name__ == "__main__":
    loader = AstronautsDataLoader()
    loader.run()