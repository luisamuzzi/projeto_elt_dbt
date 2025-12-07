import requests
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv
import os

# Carregando variáveis de ambiente
load_dotenv()

# Classe para requisitar os dados da API e salvar em um dataframe
class JobicyAPI:

    def __init__(self, base_url:str, industry:str, count:int):
        """
        Método construtor da classe

        Parâmetros:
        - base_url (str): URL base da API
        - industry (str): A industria a qual o trabalho pertence
        - count (int): Quantos registros serão extraídos
        """

        self.base_url = base_url
        self.industry = industry
        self.count = count
        self.data = None

    def fetch_data(self):
        """
        Método para requisitar os dados da API e salvar num atributo da própria classe

        Faz a requisição dos dados à API. Caso o status seja 200, salva os dados em formato json em data.
        Caso contrário, informa o erro que ocorreu.

        """
        url = f"{self.base_url}?count={self.count}&industry={self.industry}" 
        response = requests.get(url)
        if response.status_code == 200:
            self.data = response.json()
        else:
            response.raise_for_status()

    def get_jobs_data(self):
        """
        Método para carregar dados do self.data em um dataframe

        Verifica se o atributo data existe (não é vazio) e se existe a chave 'jobs' dentro dele.
        Caso sim, retorna a captura do valor associado à chave 'jobs' e salva em um dataframe.
        Caso contrário, retorna um dataframe vazio.  

        """
        if self.data and 'jobs' in self.data:
          return pd.DataFrame(self.data['jobs'])
        else:
            return pd.DataFrame()

# Classe para conectar e salvar dados no snowflake
class Snowflake:
    def __init__(self, account:str, user:str, password:str, database:str, schema:str, warehouse:str):

        """
        Método construtor da classe.

        Parâmetros:
        - account: identificador da conta
        - user: nome de usuário
        - password: senha da conta
        - database: nome da database a qual se quer conectar
        - schema: nome do schema
        - warehouse: nome de warehouse

        A engine recebe uma URL composta dos parâmetros da classe.

        """

        self.engine = create_engine(URL(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse
        ))

    def save_to_snowflake(self, df:pd.DataFrame, table_name:str):

        """
        # Método para salvar dados no snowflake a partir de um dataframe.

        Parâmetros:
        - df: dataframe com dos dados a serem salvos no snowflake.
        - table_name: nome da tabela a ser criada com os dados no snowflake.
        
        """

        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

def main():
    # Instanciar a classe
    api = JobicyAPI(
       base_url='https://jobicy.com/api/v2/remote-jobs',
       industry='data-science',
       count=100
    )

    # Requisitar dados
    api.fetch_data()

    # Salvar dados em um dataframe
    jobs_df = api.get_jobs_data()

    # Verificar se o dataframe não está vazio
    if not jobs_df.empty:

        # Ajustar nomes das colunas
        jobs_df.columns = [
            'ID', 'URL', 'jobSlug', 'jobTitle', 'companyName', 'companyLogo', 'jobIndustry',
              'jobType', 'jobGeo', 'jobLevel', 'jobExcerpt', 'jobDescription', 'pubDate', 'salaryMin',
              'salaryMax', 'salaryCurrency', 'salaryPeriod'
        ]

        # Alinhar tipos de dados
        jobs_df['ID'] = jobs_df['ID'].astype(int)
        string_colums = [
            'URL', 'jobSlug', 'jobTitle', 'companyName', 'companyLogo', 'jobIndustry',
              'jobType', 'jobGeo', 'jobLevel', 'jobExcerpt', 'jobDescription', 'pubDate', 'salaryMin',
              'salaryMax', 'salaryCurrency', 'salaryPeriod'
              ]
        for column in string_colums:
            jobs_df[column] = jobs_df[column].astype(str)

        # Tratar valores nulos
        jobs_df = jobs_df.fillna('')

        # Instanciar a classe
        saver=Snowflake(
            account=os.getenv("ACCOUNT"),
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE"),
            schema=os.getenv("SCHEMA"),
            warehouse=os.getenv("WAREHOUSE")
        )

        # Salvar dados no snowflake
        saver.save_to_snowflake(jobs_df, table_name='remote_jobs') 
        print('Dados salvos com sucesso no Snowflake!')

    else:
        print('Não existem dados a serem salvos no Snowflake.')

if __name__ == '__main__':
    main()    