# src/luigi_pipeline.py  

"""
Pipeline Luigi – Medallion Architecture (Bronze → Silver → Gold)

Este pipeline realiza:
1. Extração de dados da API (Bronze)
2. Transformação e particionamento (Silver)
3. Agregação e criação da camada Gold

Funcionalidades:
- Orquestração automática com Luigi
- Retries configuráveis em cada task
- Tratamento de erros com logging e event handler
- Idempotência (reruns seguros)
- Preparado para execução via Task Scheduler ou cron
"""

import luigi
import os
import json
import logging
from luigi import Event
from src.api.breweries_api import fetch_breweries
from src.transformations.bronze_to_silver import bronze_to_silver
from src.transformations.silver_to_gold import silver_to_gold

# ------------------------- CONFIGURAÇÃO -------------------------

# Diretório base para armazenar os dados Bronze, Silver e Gold
DATA_DIR = "data"

# Configuração de logging: INFO para progresso, ERROR para falhas
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Event handler global para capturar falhas em qualquer task
@luigi.Task.event_handler(Event.FAILURE)
def on_failure(task, exception):
    """
    Captura falhas de qualquer tarefa e registra no log.
    """
    logger.error(f"[ALERTA] Tarefa {task.__class__.__name__} falhou: {exception}")

# ------------------------- TAREFAS -------------------------

class ExtractTask(luigi.Task):
    """
    Extração de dados da API (Bronze Layer)
    
    Funcionalidades:
    - Busca dados da API via fetch_breweries()
    - Salva JSON em data/bronze/breweries_raw.json
    - Retry automático: 3 tentativas com delay de 60s
    - Idempotência: sobrescreve arquivo existente
    """
    retry_count = 3
    retry_delay = 60

    def output(self):
        """
        Define o arquivo de saída da camada Bronze
        """
        return luigi.LocalTarget(os.path.join(DATA_DIR, "bronze", "breweries_raw.json"))

    def run(self):
        """
        Executa a tarefa de extração da API
        """
        try:
            logger.info("[ExtractTask] Buscando dados da API...")
            data = fetch_breweries()
            os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
            with open(self.output().path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            logger.info("[ExtractTask] Dados salvos em Bronze.")
        except Exception as e:
            logger.exception("[ExtractTask] Erro ao buscar dados")
            raise e

class SilverTask(luigi.Task):
    """
    Transformação da camada Bronze para Silver
    
    Funcionalidades:
    - Lê JSON da camada Bronze
    - Transforma e salva em Parquet, particionado por estado/cidade
    - Cria arquivo marcador _SUCCESS para Luigi
    - Retry automático: 3 tentativas com delay de 60s
    """
    retry_count = 3
    retry_delay = 60

    def requires(self):
        """
        Depende da camada Bronze (ExtractTask)
        """
        return ExtractTask()

    def output(self):
        """
        Define o arquivo marcador de sucesso da camada Silver
        """
        return luigi.LocalTarget(os.path.join(DATA_DIR, "silver", "_SUCCESS"))

    def run(self):
        """
        Executa a transformação Bronze → Silver
        """
        try:
            logger.info("[SilverTask] Transformando dados para Silver...")
            bronze_path = self.input().path
            silver_dir = os.path.dirname(self.output().path)
            os.makedirs(silver_dir, exist_ok=True)
            bronze_to_silver(bronze_path, silver_dir)
            # Cria arquivo marcador para Luigi saber que a tarefa foi concluída
            with open(self.output().path, "w") as f:
                f.write("")
            logger.info("[SilverTask] Dados salvos em Silver.")
        except Exception as e:
            logger.exception("[SilverTask] Erro na transformação para Silver")
            raise e

class GoldTask(luigi.Task):
    """
    Agregação da camada Silver para Gold
    
    Funcionalidades:
    - Agrega dados Silver (ex: quantidade de breweries por tipo e estado)
    - Salva Parquet final em data/gold/breweries_aggregated.parquet
    - Remove arquivo antigo antes de gerar novo (idempotência)
    - Retry automático: 3 tentativas com delay de 60s
    """
    retry_count = 3
    retry_delay = 60

    def requires(self):
        """
        Depende da camada Silver (SilverTask)
        """
        return SilverTask()

    def output(self):
        """
        Define o arquivo final da camada Gold
        """
        return luigi.LocalTarget(os.path.join(DATA_DIR, "gold", "breweries_aggregated.parquet"))

    def run(self):
        """
        Executa agregação da Silver → Gold
        """
        try:
            logger.info("[GoldTask] Criando camada Gold (agregações)...")
            silver_dir = os.path.dirname(self.input().path)
            gold_file = self.output().path
            os.makedirs(os.path.dirname(gold_file), exist_ok=True)

            # Remove Gold antigo para idempotência
            if os.path.exists(gold_file):
                os.remove(gold_file)

            silver_to_gold()
            logger.info("[GoldTask] Gold layer criada com sucesso.")
        except Exception as e:
           
            logger.exception("[GoldTask] Erro na criação da Gold layer")
            raise e

# ------------------------- EXECUÇÃO -------------------------

if __name__ == "__main__":
    """
    Executa o pipeline completo:
    - Roda apenas GoldTask
    - Luigi resolve dependências automaticamente:
      GoldTask -> SilverTask -> ExtractTask
    - Pode ser agendado via Task Scheduler ou cron
    """
    #luigi.build([GoldTask()], local_scheduler=True)
    luigi.build([GoldTask()], local_scheduler=False, scheduler_host='luigi-scheduler', scheduler_port=8082)
