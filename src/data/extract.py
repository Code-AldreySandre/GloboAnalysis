import pdfplumber
from src import get_logger

class ExtratorPDFGloboAnalisys:
    """
    Classe responsável pela Extração de dados (Extract) do documento em PDF.
    Focada em localizar e extrair a Tabela 1 com segurança estrutural.
    """

    def __init__(self, caminho_pdf: str):
        self.caminho_pdf = caminho_pdf
        self.logger = get_logger(self.__class__.__name__)
        self.chaves_dicionario = [
            'id', 'estado_civil', 'grau_instrucao', 'n_filhos', 
            'salario', 'idade_bruta', 'procedencia'
        ]

    def _desmembrar_celulas_com_quebra(self, linha_bruta: list) -> list:
        """
        Resolve o problema de linhas aglutinadas no PDF (ex: "14\n15" na mesma célula).
        Se identificar quebras de linha (\n), separa os dados em múltiplas linhas virtuais.
        """
        linhas_desmembradas = []
        
        tem_aglutinacao = any('\n' in str(celula) for celula in linha_bruta if celula)

        if not tem_aglutinacao:
            return [linha_bruta] 

        try:
            celulas_divididas = [str(celula).split('\n') if celula else [''] for celula in linha_bruta]
            
            qtd_sub_linhas = max(len(celula) for celula in celulas_divididas)

            for i in range(qtd_sub_linhas):
                nova_linha = []
                for celula_div in celulas_divididas:
                    valor = celula_div[i] if i < len(celula_div) else ''
                    nova_linha.append(valor.strip())
                linhas_desmembradas.append(nova_linha)
                
            self.logger.info(f"Linha aglutinada desmembrada em {qtd_sub_linhas} registros independentes.")
            
        except Exception as e:
            self.logger.error(f"Falha ao tentar desmembrar linha aglutinada: {e}")
            linhas_desmembradas.append(linha_bruta) # Retorna o original em caso de falha

        return linhas_desmembradas

    def extrair_tabela_funcionarios(self, pagina_alvo: int = 1) -> list:
        """
        Abre o PDF, navega até a página especificada e extrai a tabela.
        Retorna uma lista de dicionários pronta para o módulo de Transformação.
        
        Args:
            pagina_alvo (int): Número da página (indexado do 0. Para página 2 do PDF, use 1).
        """
        self.logger.info(f"Iniciando leitura do PDF: {self.caminho_pdf}")
        dados_extraidos = []

        try:
            with pdfplumber.open(self.caminho_pdf) as pdf:
                if pagina_alvo >= len(pdf.pages):
                    raise ValueError(f"Página {pagina_alvo} não existe no documento.")

                pagina = pdf.pages[pagina_alvo]
                
                # Extrai a maior tabela encontrada na página
                tabela_bruta = pagina.extract_table()
                
                if not tabela_bruta:
                    self.logger.warning(f"Nenhuma tabela encontrada na página {pagina_alvo}.")
                    return dados_extraidos

                self.logger.info(f"Tabela encontrada. Total de linhas detectadas inicialmente: {len(tabela_bruta)}")

                tabela_dados = tabela_bruta[2:] 

                for i, linha in enumerate(tabela_dados):
                    if not any(linha):
                        continue

                    linhas_corrigidas = self._desmembrar_celulas_com_quebra(linha)

                    for linha_limpa in linhas_corrigidas:
                        linha_padronizada = linha_limpa + [''] * (len(self.chaves_dicionario) - len(linha_limpa))
                        
                        registro = dict(zip(self.chaves_dicionario, linha_padronizada[:7]))
                    
                        for key, value in registro.items():
                            if isinstance(value, str):
                                registro[key] = value.replace('\n', ' ').strip()
                                
                        dados_extraidos.append(registro)

            self.logger.info(f"Extração concluída com sucesso. Total de {len(dados_extraidos)} registros coletados.")
            return dados_extraidos

        except Exception as e:
            self.logger.critical(f"Erro fatal na extração do PDF: {e}")
            return []
