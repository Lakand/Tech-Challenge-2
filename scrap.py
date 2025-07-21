from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import io
from datetime import datetime

options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=options)
print("WebDriver iniciado.")

url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
driver.get(url)
print(f"Acessando URL: {url}")

try:
    wait = WebDriverWait(driver, 20)
    
    select_element = wait.until(EC.presence_of_element_located((By.ID, "segment")))
    select = Select(select_element)
    select.select_by_value("2")
    print("Critério alterado para Setor de Atuação.")
    
    wait.until(EC.element_to_be_clickable((By.ID, "selectPage")))

    select_page = Select(driver.find_element(By.ID, "selectPage"))
    select_page.select_by_visible_text("120")
    print("Resultados por página alterado para 120.")

    wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "tbody tr")) > 80)
    print(f"Tabela carregada com {len(driver.find_elements(By.CSS_SELECTOR, 'tbody tr'))} linhas.")

    html = driver.page_source
except Exception as e:
    print(f"ERRO durante a navegação/extração: {e}")
    html = None
finally:
    driver.quit()
    print("WebDriver finalizado.")

if html:
    try:
        df = pd.read_html(io.StringIO(str(html)), header=0, decimal=',', thousands='.')[0]
        
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])

        df = df[~df['Setor'].str.contains('Quantidade Teórica Total|Redutor', na=False)]
        df = df.reset_index(drop=True)
        df.rename(columns={'Setor': "setor", 'Código': "codigo", 'Ação' : "acao", 'Tipo' : "tipo",
                    'Qtde. Teórica': "qtd_teorica", 'Part. (%)': "part(%)", 'Part. (%)Acum.' : "part(%)acum"}, inplace=True)
        print("\nDataFrame processado com sucesso.")
        print(df)

        data_hoje = datetime.now()
        data_formatada = data_hoje.strftime("%Y%m%d")
        nome_arquivo_com_data = f"dados_ibov_{data_formatada}.parquet"
        
        print(f"\nSalvando o arquivo como: {nome_arquivo_com_data}")
        #df.to_parquet(nome_arquivo_com_data, index=False)
        print(f"Arquivo '{nome_arquivo_com_data}' salvo com sucesso.")

    except Exception as e:
        print(f"ERRO durante o processamento do DataFrame: {e}")
else:
    print("Não foi possível obter o HTML da página.")