#- Processo 1
# 1. Entra no FTP (172.20.24.4:21);
# 2. Pega o arquivo XXXX-XX-XX_TrackingRecord.xlsx mais recente, salva em uma pasta X;
# 3. Loga no outlook
# 4. Envia o arquivo com um texto já pré definido alterando apenas a data.

import os
from ftplib import FTP
from datetime import datetime
import win32com.client as win32
import re
from dotenv import load_dotenv

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


class AutomacaoProcesso1:
    def __init__(self):

        self.ftp_host = os.getenv('SFTP_HOST')
        self.ftp_port = int(os.getenv('SFTP_PORT'))
        self.ftp_user = os.getenv('SFTP_USER')
        self.ftp_pass = os.getenv('SFTP_PASS')
        

        self.pasta_destino = os.getenv('PASTA_TRACKING')
        

        self.destinatarios = os.getenv('DESTINATARIO_EMAIL')
        self.assunto = "Relatório Damon"
        self.corpo_email = """Bom dia,
        
Segue relatório trackingrecord do dia {data}.

Att."""
    
    def conectar_ftp(self):
        """Conecta ao servidor FTP"""
        try:
            ftp = FTP()
            ftp.connect(self.ftp_host, self.ftp_port)
            ftp.login(self.ftp_user, self.ftp_pass)
            print(f"Conectado ao FTP: {self.ftp_host}")
            return ftp
        except Exception as e:
            print(f"Erro ao conectar ao FTP: {e}")
            return None
    
    def buscar_arquivo_mais_recente(self, ftp, padrao="TrackingRecord.xlsx"):
        """Busca o arquivo TrackingRecord mais recente no FTP"""
        try:

            arquivos = []
            ftp.retrlines('LIST', arquivos.append)
            

            arquivos_tracking = []
            for linha in arquivos:
                partes = linha.split()
                if len(partes) >= 9:
                    nome_arquivo = ' '.join(partes[8:])

                    if padrao in nome_arquivo and nome_arquivo.endswith('.xlsx'):

                        match = re.match(r'(\d{4}-\d{2}-\d{2})_TrackingRecord\.xlsx', nome_arquivo)
                        if match:
                            data_arquivo = match.group(1)
                            arquivos_tracking.append((data_arquivo, nome_arquivo))
            
            if not arquivos_tracking:
                print("Nenhum arquivo TrackingRecord encontrado!")
                return None
            

            arquivos_tracking.sort(reverse=True)
            arquivo_mais_recente = arquivos_tracking[0][1]
            
            print(f"Arquivo mais recente encontrado: {arquivo_mais_recente}")
            return arquivo_mais_recente
            
        except Exception as e:
            print(f"Erro ao buscar arquivos: {e}")
            return None
    
    def baixar_arquivo(self, ftp, nome_arquivo):
        """Baixa o arquivo do FTP para a pasta local"""
        try:
            caminho_local = os.path.join(self.pasta_destino, nome_arquivo)
            
            with open(caminho_local, 'wb') as arquivo_local:
                ftp.retrbinary(f'RETR {nome_arquivo}', arquivo_local.write)
            
            print(f"Arquivo baixado: {caminho_local}")
            return caminho_local
            
        except Exception as e:
            print(f"Erro ao baixar arquivo: {e}")
            return None
    
    def extrair_data_arquivo(self, nome_arquivo):
        """Extrai a data do nome do arquivo"""
        match = re.match(r'(\d{4}-\d{2}-\d{2})_TrackingRecord\.xlsx', nome_arquivo)
        if match:
            data_str = match.group(1)

            data_obj = datetime.strptime(data_str, '%Y-%m-%d')
            return data_obj.strftime('%d/%m/%Y')
        return datetime.now().strftime('%d/%m/%Y')
    
    def enviar_email_outlook(self, caminho_arquivo, nome_arquivo):
        """Envia email via Outlook com o arquivo anexado"""
        try:
            outlook = win32.Dispatch('outlook.application')
            email = outlook.CreateItem(0)  
            

            data_formatada = self.extrair_data_arquivo(nome_arquivo)
            

            email.To = self.destinatarios
            email.Subject = self.assunto.format(data=data_formatada)
            email.Body = self.corpo_email.format(data=data_formatada)
            

            email.Attachments.Add(caminho_arquivo)
            

            email.Send()
            
            print(f"Email enviado com sucesso para: {self.destinatarios}")
            return True
            
        except Exception as e:
            print(f"Erro ao enviar email: {e}")
            return False
    
    def executar(self):
        """Executa o processo completo"""
        print("=" * 50)
        print("INICIANDO PROCESSO 1 - FTP + EMAIL")
        print("=" * 50)
              

        ftp = self.conectar_ftp()
        if not ftp:
            return False
        
        try:

            nome_arquivo = self.buscar_arquivo_mais_recente(ftp)
            if not nome_arquivo:
                return False
            

            caminho_arquivo = self.baixar_arquivo(ftp, nome_arquivo)
            if not caminho_arquivo:
                return False
            

            sucesso = self.enviar_email_outlook(caminho_arquivo, nome_arquivo)
            
            print("=" * 50)
            if sucesso:
                print("PROCESSO 1 CONCLUÍDO COM SUCESSO!")
            else:
                print("PROCESSO 1 CONCLUÍDO COM ERROS NO EMAIL")
            print("=" * 50)
            
            return sucesso
            
        finally:

            ftp.quit()
            print("Conexão FTP encerrada")



if __name__ == "__main__":
    automacao = AutomacaoProcesso1()
    automacao.executar()