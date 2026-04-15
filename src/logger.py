import logging
import os 
from datetime import datetime

def get_logger(nome_modulo):
    """
    Configura e retorna um logger padronizado para o projeto.
    
    Args:
        nome_modulo (str): O nome do módulo que está chamando o logger (usualmente __name__).
        
    Returns:
        logging.Logger: Instância configurada do logger.
    """

    os.makedirs('logs', exist_ok=True)

    logger = logging.getLogger(nome_modulo)

    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter( 
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    data_atual = datetime.now().strftime('%Y%m%d')
    caminho_arquivo_log = f"logs/execucao_{data_atual}.log"

    file_handler = logging.FileHandler(caminho_arquivo_log, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger