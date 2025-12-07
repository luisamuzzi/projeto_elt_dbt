import requests
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv
import os

class JobicyAPI:

    # Definindo os atributos da classe
    def __init__(self, base_url:str, industry:str, count:int):
        self.base_url = base_url
        self.industry = industry
        self.count = count
        self.data = None

    # Método para requisitar os dados da API e salvar num atributo da própria classe
    def fetch_data(self):
        url = f"{self.base_url}?count={self.count}&industry={self.industry}" 
        response = requests.get(url)
        if response.status_code == 200:
            self.data = response.json()
        else:
            response.raise_for_status()

    # Método para carregar dados do self.data em um dataframe
    def get_jobs_data(self):
        if self.data and 'jobs' in self.data:
          return pd.DataFrame(self.data['jobs'])
        else:
            return pd.DataFrame()

def main():
    api = JobicyAPI(
       base_url='https://jobicy.com/api/v2/remote-jobs',
       industry='data-science',
       count=10 
    )
    api.fetch_data()

    jobs_df = api.get_jobs_data()

    print(jobs_df)  

if __name__ == '__main__':
    main()    