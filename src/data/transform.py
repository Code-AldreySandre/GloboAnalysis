import pandas as pd
import numpy as np
from src import get_logger

class ProcessadorGloboAnalisys:
    """
    Classe responsável pelo pipeline de Transformação (ETL) dos dados dos 55 funcionários 
    da Empresa GloboAnalisys[cite: 12].
    """
    
    def __init__(self, dados_brutos: list):
        """
        Inicializa o processador com os dados brutos extraídos do PDF.
        
        Args:
            dados_brutos (list): Lista de dicionários contendo as informações da Tabela 1.
        """
        self.logger = get_logger(self.__class__.__name__)
        self.dados_brutos = dados_brutos
        self.df_processado = None
        self.erros = 0

    def _tratar_idade(self, idade_bruta: str):
        """
        Separa a string de idade em anos e meses conforme a estrutura da Tabela 1.
        
        Exemplo: "04 43" -> Anos: 43, Meses: 4
        """
        try:
            partes = str(idade_bruta).split()
            meses = float(partes[0]) if len(partes) > 0 else np.nan
            anos = float(partes[1]) if len(partes) > 1 else np.nan
            return anos, meses
        except (ValueError, IndexError):
            return np.nan, np.nan

    def _tratar_filhos(self, estado_civil: str, filhos_bruto: str):
        """
        Aplica a lógica de negócio para a variável quantitativa 'Número de filhos'.
        A pergunta não foi feita para funcionários solteiros.
        """
        estado_civil_norm = str(estado_civil).strip().capitalize()
        filhos_str = str(filhos_bruto).strip()
        
       
        if estado_civil_norm == 'Solteiro' or filhos_str in ['', '|', '||', 'NA']:
            return np.nan
        
        try:
            return float(filhos_str)
        except ValueError:
            return np.nan

    def _tratar_salario(self, salario_bruto: str):
        """
        Converte o salário (fração do salário mínimo) para float[cite: 12].
        Lida com inconsistências de ponto/vírgula na extração do PDF.
        """
        if not salario_bruto:
            return np.nan
            
        salario_limpo = str(salario_bruto).replace(',', '.')
        try:
            return float(salario_limpo)
        except ValueError:
            return np.nan

    def processar(self) -> pd.DataFrame:
        """
        Executa a transformação completa dos dados, estruturando o banco de dados final.
        """
        self.logger.info(f"Iniciando pipeline de transformação para {len(self.dados_brutos)} registros.")
        dados_limpos = []

        for i, linha in enumerate(self.dados_brutos):
            try:
                anos, meses = self._tratar_idade(linha.get('idade_bruta', ''))
                
                registro = {
                    'ID': linha.get('id'),
                    'Estado_Civil': str(linha.get('estado_civil', '')).strip().capitalize(),
                    'Grau_Instrucao': str(linha.get('grau_instrucao', '')).strip().capitalize(),
                    'N_Filhos': self._tratar_filhos(linha.get('estado_civil'), linha.get('n_filhos')),
                    'Salario_SM': self._tratar_salario(linha.get('salario')),
                    'Idade_Anos': anos,
                    'Idade_Meses': meses,
                    'Regiao_Procedencia': str(linha.get('procedencia', '')).strip().capitalize()
                }
                dados_limpos.append(registro)

            except Exception as e:
                self.erros += 1
                id_atual = linha.get('id', f'Indice_{i}')
                self.logger.error(f"Falha ao processar registro ID {id_atual}: {e}")

        self.df_processado = pd.DataFrame(dados_limpos)
        
        nulos = self.df_processado.isnull().sum().sum()
        self.logger.info(f"Processamento finalizado. Sucesso: {len(self.df_processado)} | Falhas: {self.erros}")
        self.logger.info(f"Total de valores nulos/NA gerados: {nulos}")
        
        return self.df_processado

    def exportar_csv(self, caminho_saida: str):
        """
        Exporta os dados para CSV usando ponto e vírgula como separador e vírgula para decimais.
        Isso facilita a abertura direta no Excel e respeita a preferência de análise.
        """
        if self.df_processado is None:
            self.logger.error("Falha na exportação: O DataFrame ainda não foi processado.")
            return

        try:
            # Configuração de separadores para padrão brasileiro (CSV de intercâmbio)
            self.df_processado.to_csv(caminho_saida, index=False, sep=';', decimal=',')
            self.logger.info(f"Arquivo CSV exportado com sucesso para: {caminho_saida}")
        except Exception as e:
            self.logger.critical(f"Erro crítico ao salvar arquivo em disco: {e}")