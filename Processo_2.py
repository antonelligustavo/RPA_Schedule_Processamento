#- Processo 2
# 1. Entra no SFTP (177.126.179.190:22);
# 2. Monitora as seguintes pastas:
#  2.1. /home/sftp/uol/Inboxnetp
#  2.2. /home/sftp/uol/TrackingOPL
#  2.3. /home/sftp/uol/Outboxnetp
#  2.4. /home/sftp/uol/TrackingTransporte
# 3. Efetua a validação se possui algum arquivo nas pastas;
# 4. Salva um LOG com o que tem dentro de cada pasta.

import os
from datetime import datetime
import paramiko
import stat
import requests
import json
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
# Busca o .env na mesma pasta do script
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

class AutomacaoProcesso2:
    def __init__(self):

        self.sftp_host = os.getenv('SFTP_HOST_2')
        self.sftp_port = int(os.getenv('SFTP_PORT_2', '22'))
        self.sftp_user = os.getenv('SFTP_USER_2')
        self.sftp_pass = os.getenv('SFTP_PASS_2')
        

        self.pastas_monitorar = [
            "/home/sftp/uol/Inboxnetp",
            "/home/sftp/uol/TrackingOPL",
            "/home/sftp/uol/Outboxnetp",
            "/home/sftp/uol/TrackingTransporte"
        ]
        

        self.pasta_logs = os.getenv('PASTA_LOGS')
        

        self.teams_webhook_url = os.getenv('TEAMS_WEBHOOK_URL')
        

        self._validar_variaveis()
    
    def _validar_variaveis(self):
        """Valida se todas as variáveis necessárias foram carregadas"""
        variaveis_obrigatorias = {
            'SFTP_HOST_2': self.sftp_host,
            'SFTP_USER_2': self.sftp_user,
            'SFTP_PASS_2': self.sftp_pass,
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
    
    def conectar_sftp(self):
        """Conecta ao servidor SFTP"""
        try:
            # Cria cliente SSH
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Conecta ao servidor
            ssh.connect(
                hostname=self.sftp_host,
                port=self.sftp_port,
                username=self.sftp_user,
                password=self.sftp_pass
            )
            

            sftp = ssh.open_sftp()
            print(f"Conectado ao SFTP: {self.sftp_host}")
            
            return ssh, sftp
            
        except Exception as e:
            print(f"Erro ao conectar ao SFTP: {e}")
            return None, None
    
    def listar_arquivos_pasta(self, sftp, caminho_pasta):
        """Lista todos os arquivos em uma pasta do SFTP"""
        try:
            arquivos = sftp.listdir(caminho_pasta)
            

            arquivos_detalhados = []
            for arquivo in arquivos:
                caminho_completo = f"{caminho_pasta}/{arquivo}"
                try:
                    attrs = sftp.stat(caminho_completo)

                    if not stat.S_ISDIR(attrs.st_mode):

                        data_modificacao = datetime.fromtimestamp(attrs.st_mtime)
                        tamanho_kb = attrs.st_size / 1024  
                        
                        arquivos_detalhados.append({
                            'nome': arquivo,
                            'tamanho_kb': round(tamanho_kb, 2),
                            'data_modificacao': data_modificacao.strftime('%d/%m/%Y %H:%M:%S')
                        })
                except:

                    arquivos_detalhados.append({
                        'nome': arquivo,
                        'tamanho_kb': 'N/A',
                        'data_modificacao': 'N/A'
                    })
            
            return arquivos_detalhados
            
        except Exception as e:
            print(f"Erro ao listar arquivos em {caminho_pasta}: {e}")
            return None
    
    def gerar_log(self, resultados):
        """Gera arquivo de log com os resultados do monitoramento"""
        try:

            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            nome_log = f"MonitoramentoSFTP_{timestamp}.log"
            caminho_log = os.path.join(self.pasta_logs, nome_log)
            
            with open(caminho_log, 'w', encoding='utf-8') as log:
                log.write("=" * 80 + "\n")
                log.write(f"LOG DE MONITORAMENTO SFTP - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                log.write("=" * 80 + "\n\n")
                
                for resultado in resultados:
                    pasta = resultado['pasta']
                    arquivos = resultado['arquivos']
                    
                    log.write(f"\n{'=' * 80}\n")
                    log.write(f"PASTA: {pasta}\n")
                    log.write(f"{'=' * 80}\n")
                    
                    if arquivos is None:
                        log.write("ERRO: Não foi possível acessar a pasta\n")
                    elif len(arquivos) == 0:
                        log.write("STATUS: Pasta vazia (sem arquivos)\n")
                    else:
                        log.write(f"STATUS: {len(arquivos)} arquivo(s) encontrado(s)\n\n")
                        
                        for i, arquivo in enumerate(arquivos, 1):
                            log.write(f"  [{i}] Arquivo: {arquivo['nome']}\n")
                            log.write(f"      Tamanho: {arquivo['tamanho_kb']} KB\n")
                            log.write(f"      Última modificação: {arquivo['data_modificacao']}\n\n")
                
                log.write("\n" + "=" * 80 + "\n")
                log.write("FIM DO LOG\n")
                log.write("=" * 80 + "\n")
            
            print(f"Log gerado: {caminho_log}")
            return caminho_log
            
        except Exception as e:
            print(f"Erro ao gerar log: {e}")
            return None

    def enviar_para_teams(self, resultados):
        """Envia resumo do monitoramento para o Teams via Adaptive Card"""
        try:
            timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

            total_arquivos = 0
            pastas_com_erro = 0
            pastas_vazias = 0

            facts_adaptive = []

            for resultado in resultados:
                pasta = resultado['pasta']
                arquivos = resultado['arquivos']
                nome_pasta = pasta.split('/')[-1]

                if arquivos is None:
                    status = "❌ ERRO"
                    qtd_arquivos = "N/A"
                    pastas_com_erro += 1
                elif len(arquivos) == 0:
                    status = "⚪ Vazia"
                    qtd_arquivos = "0"
                    pastas_vazias += 1
                else:
                    status = "✅ OK"
                    qtd_arquivos = str(len(arquivos))
                    total_arquivos += len(arquivos)

                facts_adaptive.append({
                    "title": f"📁 {nome_pasta}",
                    "value": f"{status} - {qtd_arquivos} arquivo(s)"
                })

            # Define estilo visual da carta
            if pastas_com_erro > 0:
                container_style = "attention"
                status_geral = "⚠️ ATENÇÃO"
            elif total_arquivos > 0:
                container_style = "warning"
                status_geral = "📊 Arquivos Detectados"
            else:
                container_style = "good"
                status_geral = "✅ Tudo OK"

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
                                "text": f"🔍 Monitoramento SFTP - {status_geral}"
                            },
                            {
                                "type": "TextBlock",
                                "isSubtle": True,
                                "wrap": True,
                                "spacing": "None",
                                "text": f"**Execução:** {timestamp}\nServidor: {self.sftp_host}"
                            },
                            {
                                "type": "Container",
                                "style": container_style,
                                "items": [
                                {"type": "FactSet", "facts": facts_adaptive}
                                ]
                            },
                            {
                                "type": "TextBlock",
                                "wrap": True,
                                "text": f"**Total de arquivos:** {total_arquivos} | **Pastas vazias:** {pastas_vazias} | **Erros:** {pastas_com_erro}"
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
    
    def gerar_resumo_console(self, resultados):
        """Exibe resumo no console"""
        print("\n" + "=" * 80)
        print("RESUMO DO MONITORAMENTO")
        print("=" * 80)
        
        for resultado in resultados:
            pasta = resultado['pasta']
            arquivos = resultado['arquivos']
            
            if arquivos is None:
                status = "ERRO AO ACESSAR"
                qtd = "N/A"
            elif len(arquivos) == 0:
                status = "VAZIA"
                qtd = "0"
            else:
                status = "OK"
                qtd = str(len(arquivos))
            
            print(f"  • {pasta.split('/')[-1]:25s} | Arquivos: {qtd:4s} | Status: {status}")
        
        print("=" * 80)
    
    def executar(self):
        """Executa o processo completo de monitoramento"""
        print("=" * 80)
        print("INICIANDO PROCESSO 2 - MONITORAMENTO SFTP")
        print("=" * 80)
        

        self.criar_pasta_logs()
        

        ssh, sftp = self.conectar_sftp()
        if not sftp:
            return False
        
        try:

            resultados = []
            
            for pasta in self.pastas_monitorar:
                print(f"\nMonitorando: {pasta}")
                arquivos = self.listar_arquivos_pasta(sftp, pasta)
                
                resultados.append({
                    'pasta': pasta,
                    'arquivos': arquivos
                })
                
                if arquivos is None:
                    print(f"  ❌ Erro ao acessar pasta")
                elif len(arquivos) == 0:
                    print(f"  ✓ Pasta vazia")
                else:
                    print(f"  ✓ {len(arquivos)} arquivo(s) encontrado(s)")
            

            caminho_log = self.gerar_log(resultados)
            

            print("\n📤 Enviando resumo para o Teams...")
            self.enviar_para_teams(resultados)
            

            self.gerar_resumo_console(resultados)
            
            print("\n" + "=" * 80)
            print("PROCESSO 2 CONCLUÍDO COM SUCESSO!")
            print("=" * 80)
            
            return True
            
        finally:

            sftp.close()
            ssh.close()
            print("Conexão SFTP encerrada")



if __name__ == "__main__":
    processo2 = AutomacaoProcesso2()
    processo2.executar()