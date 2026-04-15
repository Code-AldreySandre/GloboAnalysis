import os
from src.data.extract import ExtratorPDFGloboAnalisys
from src.data.transform import ProcessadorGloboAnalisys

def executar_pipeline():
    caminho_pdf = 'data/raw/ATIVIDADE 1 - VARIAVEIS QUANTITATIVAS ALUNOS.pdf'
    caminho_csv = 'data/processed/dados_funcionarios_processados.csv'
    
    # Checagem de segurança
    if not os.path.exists(caminho_pdf):
        print(f"Não encontrei o PDF no caminho: {caminho_pdf}")
        print("Certifique-se de que ele está dentro da pasta data/01_raw/")
        return

    print("Iniciando o pipeline de extração da GloboAnalisys...\n")

    # Passo 1: Extração (Lembrando que pagina_alvo=1 é a página 2 do PDF)
    extrator = ExtratorPDFGloboAnalisys(caminho_pdf)
    dados_brutos = extrator.extrair_tabela_funcionarios(pagina_alvo=1)

    if not dados_brutos:
        print("A extração retornou vazia. Verifique os logs para mais detalhes.")
        return

    processador = ProcessadorGloboAnalisys(dados_brutos)
    processador.processar()

    processador.exportar_csv(caminho_csv)
    
    print(f"\nSucesso! O arquivo final foi salvo em: {caminho_csv}")

if __name__ == "__main__":
    executar_pipeline()