#- Processo 5
# 1. Conecta no GA (https://ga.flashcourier.com.br/logs);
# 2. Pesquisa o processo - SODEXO_NEW_SEMDUPLICADO_REM
# 3. Extrai um relatorio;
# 4. Valida se chegou algum arquivo;
# 5. Gera um log com o nome do arquivo e quantidade.

import os
from datetime import datetime
import requests
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
from pathlib import Path


script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(script_dir, '.env')

print(f"🔍 Procurando arquivo .env em: {env_path}")

if os.path.exists(env_path):
    print(f"✅ Arquivo .env encontrado!")
    load_dotenv(env_path)
else:
    print(f"❌ ERRO: Arquivo .env NÃO encontrado em: {env_path}")
    print(f"📁 Certifique-se de criar o arquivo .env na mesma pasta do script!")
    exit(1)

class AutomacaoProcesso5:
    def __init__(self):

        self.ga_url = os.getenv('GA_URL', 'https://ga.flashcourier.com.br/logs')
        self.ga_email = os.getenv('GA_EMAIL')
        self.ga_senha = os.getenv('GA_SENHA')
        

        self.cliente_pesquisa = "SODEXO_NEW_SEMDUPLICADO_REM"
        

        self.download_path = str(Path.home() / "Downloads")
        

        self.pasta_logs = os.getenv('PASTA_LOGS')
        

        self.teams_webhook_url = os.getenv('TEAMS_WEBHOOK_URL')
        

        self.driver = None
        self.wait = None
        self.timestamp_inicio = None
        self.arquivos_processados = []
        

        self._validar_variaveis()
    
    def _validar_variaveis(self):
        """Valida se todas as variáveis necessárias foram carregadas"""
        variaveis_obrigatorias = {
            'GA_EMAIL_5': self.ga_email,
            'GA_SENHA_5': self.ga_senha,
            'PASTA_LOGS': self.pasta_logs,
            'TEAMS_WEBHOOK_URL': self.teams_webhook_url
        }
        
        faltando = [var for var, valor in variaveis_obrigatorias.items() if not valor]
        
        if faltando:
            raise ValueError(f"❌ Variáveis faltando no arquivo .env: {', '.join(faltando)}")
        
        print("✅ Todas as variáveis de ambiente carregadas com sucesso!")
        
    def criar_pasta_logs(self):
        """Cria a pasta de logs se não existir"""
        if not os.path.exists(self.pasta_logs):
            os.makedirs(self.pasta_logs)
            print(f"Pasta de logs criada: {self.pasta_logs}")
    
    def inicializar_driver(self):
        """Inicializa o driver Chrome"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            prefs = {
                "download.default_directory": self.download_path,
                "download.prompt_for_download": False,
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.wait = WebDriverWait(self.driver, 15)
            
            print("✅ Driver Chrome iniciado")
            return True
        
        except Exception as e:
            print(f"❌ Erro ao inicializar driver: {e}")
            return False
    
    def fazer_login(self):
        """Faz login no GA"""
        try:
            print(f"🔗 Acessando GA: {self.ga_url}")
            self.driver.get(self.ga_url)
            time.sleep(2)
            
            self.timestamp_inicio = time.time()
            
            usuario_box = self.driver.find_element(By.NAME, "email")
            usuario_box.send_keys(self.ga_email)
            
            senha_box = self.driver.find_element(By.NAME, "password")
            senha_box.send_keys(self.ga_senha)
            
            login_button = self.driver.find_element(By.XPATH, '//*[@id="login"]/section/form/div[3]/button')
            login_button.click()
            
            time.sleep(3)
            print("✅ Login realizado com sucesso")
            return True
        
        except Exception as e:
            print(f"❌ Erro ao fazer login: {e}")
            return False
    
    def extrair_relatorio_cliente(self):
        """Extrai o relatório do cliente no GA"""
        try:
            print(f"🔍 Extraindo relatório para: {self.cliente_pesquisa}")
            
            campo_pesquisa = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[aria-controls='dataTableBuilder']"))
            )
            campo_pesquisa.clear()
            campo_pesquisa.send_keys(self.cliente_pesquisa)
            
            print(f"⏳ Aguardando 5 segundos...")
            time.sleep(5)
            
            botao_excel = self.wait.until(
                EC.element_to_be_clickable((By.ID, "spreadsheet"))
            )
            botao_excel.click()
            
            print("📥 Download iniciado...")
            time.sleep(7)
            
            resultado = self._processar_arquivo_excel()
            
            return resultado
        
        except Exception as e:
            print(f"❌ Erro ao extrair relatório: {e}")
            return None
    
    def _obter_arquivo_recente(self):
        """Obtém o arquivo mais recente baixado"""
        try:
            time.sleep(2)
            
            arquivos_xlsx = [f for f in os.listdir(self.download_path) 
                           if f.endswith('.xlsx') and not f.startswith('~')]
            
            if not arquivos_xlsx:
                print("⚠️ Nenhum arquivo .xlsx encontrado em Downloads")
                return None
            
            arquivos_novos = []
            agora = time.time()
            
            for arquivo in arquivos_xlsx:
                caminho_completo = os.path.join(self.download_path, arquivo)
                tempo_modificacao = os.path.getmtime(caminho_completo)
                
                if tempo_modificacao > self.timestamp_inicio and (agora - tempo_modificacao) < 30:
                    arquivos_novos.append(arquivo)
                    print(f"📄 Arquivo candidato: {arquivo} (modificado há {int(agora - tempo_modificacao)}s)")
            
            if not arquivos_novos:
                print("⚠️ Nenhum arquivo novo encontrado em Downloads")
                return None
            
            arquivo_mais_recente = max(
                arquivos_novos,
                key=lambda f: os.path.getmtime(os.path.join(self.download_path, f))
            )
            
            print(f"✅ Arquivo selecionado: {arquivo_mais_recente}")
            return arquivo_mais_recente
        
        except Exception as e:
            print(f"❌ Erro ao identificar arquivo: {e}")
            return None
    
    def _processar_arquivo_excel(self):
        """Processa o arquivo Excel baixado"""
        try:
            arquivo = self._obter_arquivo_recente()
            
            if not arquivo:
                print("⚠️ Nenhum arquivo foi identificado")
                return None
            
            arquivo_path = os.path.join(self.download_path, arquivo)
            
            if not os.path.exists(arquivo_path):
                print(f"⚠️ Arquivo não encontrado: {arquivo_path}")
                return None
            
            print(f"📊 Processando arquivo: {arquivo}")
            df = pd.read_excel(arquivo_path)
            print(f"✅ Arquivo carregado com {len(df)} linhas e {df.shape[1]} colunas")
            
            total = 0
            
            if df.shape[1] >= 7:
                coluna_d = df.iloc[:, 3]
                coluna_e = df.iloc[:, 4]
                coluna_g = df.iloc[:, 6]
                
                filtro = (coluna_g.astype(str).str.upper() == "ENTREGUE") & \
                        (~coluna_d.astype(str).str.contains(".SD1", case=False, na=False))
                
                total = int(coluna_e[filtro].sum())
                print(f"📈 Total somado da coluna E: {total}")
            else:
                print("⚠️ Arquivo não possui as colunas necessárias")
            
            self.arquivos_processados.append(arquivo)
            

            try:
                os.remove(arquivo_path)
                print(f"🗑️ Arquivo excluído: {arquivo}")
            except Exception as e:
                print(f"⚠️ Não foi possível excluir o arquivo: {e}")
            
            return {
                'total': total,
                'arquivo': arquivo
            }
        
        except Exception as e:
            print(f"❌ Erro ao processar Excel: {e}")
            return None
    
    def fechar_driver(self):
        """Fecha o driver do navegador"""
        try:
            if self.driver:
                self.driver.quit()
                print("✅ Driver fechado")
        except Exception as e:
            print(f"⚠️ Erro ao fechar driver: {e}")
    
    def extrair_relatorio_ga(self):
        """Conecta no GA e extrai o relatório"""
        try:

            if not self.inicializar_driver():
                return None
            

            if not self.fazer_login():
                self.fechar_driver()
                return None
            

            resultado = self.extrair_relatorio_cliente()
            

            self.fechar_driver()
            
            return resultado
            
        except Exception as e:
            print(f"❌ Erro ao extrair relatório do GA: {e}")
            self.fechar_driver()
            return None
    
    def gerar_log(self, resultado):
        """Gera arquivo de log com os resultados da extração"""
        try:

            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            nome_log = f"ExtracacaoGA_{timestamp}.log"
            caminho_log = os.path.join(self.pasta_logs, nome_log)
            
            with open(caminho_log, 'w', encoding='utf-8') as log:
                log.write("=" * 80 + "\n")
                log.write(f"LOG DE EXTRAÇÃO GA - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                log.write("=" * 80 + "\n\n")
                
                log.write(f"URL GA: {self.ga_url}\n")
                log.write(f"Processo Pesquisado: {self.cliente_pesquisa}\n")
                log.write("=" * 80 + "\n\n")
                
                if resultado is None:
                    log.write("STATUS: ERRO ao extrair relatório\n")
                    log.write("DETALHES: Não foi possível conectar ou extrair dados do GA\n")
                elif resultado['total'] == 0:
                    log.write(f"STATUS: ⚠️ ALERTA - Nenhum arquivo recebido\n\n")
                    log.write(f"ARQUIVO PROCESSADO: {resultado['arquivo']}\n")
                    log.write(f"QUANTIDADE TOTAL: {resultado['total']}\n")
                    log.write(f"ALERTA: Não foram recebidos arquivos hoje!\n")
                else:
                    log.write(f"STATUS: ✅ Extração realizada com SUCESSO\n\n")
                    log.write(f"ARQUIVO PROCESSADO: {resultado['arquivo']}\n")
                    log.write(f"QUANTIDADE TOTAL: {resultado['total']}\n")
                    log.write(f"ARQUIVO EXCLUÍDO: Sim (após processamento)\n")
                
                log.write("\n" + "=" * 80 + "\n")
                log.write("FIM DO LOG\n")
                log.write("=" * 80 + "\n")
            
            print(f"📄 Log gerado: {caminho_log}")
            return caminho_log
            
        except Exception as e:
            print(f"❌ Erro ao gerar log: {e}")
            return None

    def enviar_para_teams(self, resultado):
        """Envia resumo da extração para o Teams via Adaptive Card"""
        try:
            timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            

            if resultado is None:

                container_style = "attention"
                status_geral = "❌ ERRO"
                titulo = "📋 Monitoramento Sodexo - ❌ ERRO"
                quantidade_texto = "N/A"
                arquivo_texto = "Erro ao extrair"
            elif resultado['total'] == 0:

                container_style = "attention"
                status_geral = "❌ Arquivo NÃO recebido"
                titulo = "📋 Monitoramento Sodexo - ❌ Arquivo NÃO recebido"
                quantidade_texto = "0"
                arquivo_texto = "Nenhum arquivo recebido hoje"
            else:

                container_style = "good"
                status_geral = "✅ SUCESSO"
                titulo = "📋 Monitoramento Sodexo - ✅ SUCESSO"
                quantidade_texto = str(resultado['total'])
                arquivo_texto = resultado['arquivo']
            
            adaptive_payload = {
                "type": "message",
                "attachments": [{
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "weight": "Bolder",
                                "size": "Medium",
                                "text": titulo
                            },
                            {
                                "type": "TextBlock",
                                "isSubtle": True,
                                "wrap": True,
                                "spacing": "None",
                                "text": f"**Execução:** {timestamp}\nProcesso: {self.cliente_pesquisa}"
                            },
                            {
                                "type": "Container",
                                "style": container_style,
                                "items": [
                                    {
                                        "type": "FactSet",
                                        "facts": [
                                            {
                                                "title": "📈 Quantidade Total:",
                                                "value": quantidade_texto
                                            },
                                            {
                                                "title": "📁 Nomenclatura Monitorada:",
                                                "value": "SODEXO_NEW_SEMDUPLICADO_REM"
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                }]
            }

            response = requests.post(self.teams_webhook_url, json=adaptive_payload, timeout=15)

            if response.status_code == 202:
                print("✅ Mensagem (Adaptive Card) enviada para o Teams com sucesso!")
                return True
            else:
                print(f"❌ Erro ao enviar para o Teams: {response.status_code}")
                print(f"Resposta: {response.text}")
                return False

        except Exception as e:
            print(f"❌ Erro ao enviar mensagem para o Teams: {e}")
            return False
    
    def gerar_resumo_console(self, resultado):
        """Exibe resumo no console"""
        print("\n" + "=" * 80)
        print("RESUMO DA EXTRAÇÃO")
        print("=" * 80)
        
        if resultado is None:
            print("  ❌ Status: ERRO ao extrair relatório")
            print("  📊 Quantidade: N/A")
            print("  📁 Arquivo: Não processado")
        elif resultado['total'] == 0:
            print(f"  ⚠️ Status: ALERTA - Nenhum arquivo recebido")
            print(f"  📊 Quantidade Total: {resultado['total']}")
            print(f"  📁 Arquivo Processado: {resultado['arquivo']}")
            print(f"  🗑️ Arquivo Excluído: Sim")
            print(f"  ⚠️ ATENÇÃO: Não foram recebidos arquivos hoje!")
        else:
            print(f"  ✅ Status: Extração realizada com sucesso")
            print(f"  📊 Quantidade Total: {resultado['total']}")
            print(f"  📁 Arquivo Processado: {resultado['arquivo']}")
            print(f"  🗑️ Arquivo Excluído: Sim")
        
        print("=" * 80)
    
    def executar(self):
        """Executa o processo completo de extração do GA"""
        print("=" * 80)
        print("INICIANDO PROCESSO 5 - EXTRAÇÃO GA")
        print("=" * 80)
        

        self.criar_pasta_logs()
        

        resultado = self.extrair_relatorio_ga()
        

        caminho_log = self.gerar_log(resultado)
        

        print("\n📤 Enviando resumo para o Teams...")
        self.enviar_para_teams(resultado)
        

        self.gerar_resumo_console(resultado)
        
        print("\n" + "=" * 80)
        print("PROCESSO 5 CONCLUÍDO!")
        print("=" * 80)
        
        return resultado is not None and (resultado['total'] > 0 if resultado else False)



if __name__ == "__main__":
    processo5 = AutomacaoProcesso5()
    processo5.executar()