import paramiko
import requests
import os
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ===== CONFIG SFTP =====
SSH_HOST = os.getenv("SFTP_HOST_2")
SSH_PORT = int(os.getenv("SFTP_PORT_2"))
SSH_USER = os.getenv("SFTP_USER_2")
SSH_KEY  = os.getenv("SFTP_PASS_2")
TEAMS_URL = os.getenv("TEAMS_WEBHOOK_URL")

if not all([SSH_HOST, SSH_PORT, SSH_USER, SSH_KEY, TEAMS_URL]):
    raise RuntimeError("❌ Variáveis de ambiente SFTP não carregadas corretamente")

# ===== CONFIG PROCESSO =========
PASTA = "/flash2005/arquivos/auto"
MINUTOS = 15
LIMITE = 10

# ===== CONTROLE DE ALERTA =====
BASE_DIR = Path(__file__).resolve().parent
LOCK_FILE = BASE_DIR / "alerta_arquivos_parados.lock"
INTERVALO_ALERTA = 15 * 60  # 15 minutos

def pode_enviar_alerta():
    if not LOCK_FILE.exists():
        return True
    return (time.time() - LOCK_FILE.stat().st_mtime) >= INTERVALO_ALERTA

def registrar_envio():
    LOCK_FILE.write_text(str(time.time()))

def limpar_lock():
    if LOCK_FILE.exists():
        LOCK_FILE.unlink()

# ===== CONECTA NO SSH =====
print("Conectando no SSH...")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    ssh.connect(
        hostname=SSH_HOST,
        port=SSH_PORT,
        username=SSH_USER,
        password=SSH_KEY,
        timeout=10,
        banner_timeout=10,
        auth_timeout=10
    )
    print("SSH conectado com sucesso")
except Exception as e:
    print("ERRO SSH:", e)
    exit(1)

# ===== COMANDO CMD =====
cmd = (
    f"find {PASTA} -maxdepth 1 -type f -mmin +{MINUTOS} "
    f"-printf '%f\\n' 2>/dev/null | head -n {LIMITE * 2}"
)

stdin, stdout, stderr = ssh.exec_command(cmd)
saida = stdout.read().decode().strip().splitlines()
ssh.close()

# ===== PROCESSA RESULTADO =====
arquivos = [linha.strip() for linha in saida if linha.strip()]
total = len(arquivos)
arquivos_listados = arquivos[:LIMITE]

# ===== SEM PROBLEMA =====
if total == 0:
    print("Nenhum arquivo parado. Ambiente normal.")
    limpar_lock()
    exit(0)

# ===== TEM PROBLEMA, MAS JÁ AVISOU RECENTEMENTE =====
if not pode_enviar_alerta():
    print("Arquivos parados detectados, mas alerta já enviado recentemente.")
    exit(0)

# ===== DEFINE GRAVIDADE =====
if total >= 1000:
    cor = "Attention"
    emoji = "🔴"
    status = "CRÍTICO"
elif total >= 300:
    cor = "Warning"
    emoji = "⚠️"
    status = "ALERTA"
else:
    cor = "Good"
    emoji = "⚡"
    status = "ATENÇÃO"

# ===== FORMATA DADOS =====
agora = datetime.now().strftime("%d/%m/%Y às %H:%M:%S")

# ===== PAYLOAD TEAMS =====
payload = {
    "type": "message",
    "attachments": [{
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                # Cabeçalho colorido
                {
                    "type": "Container",
                    "style": cor,
                    "items": [
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column",
                                    "width": "auto",
                                    "items": [{
                                        "type": "TextBlock",
                                        "text": emoji,
                                        "size": "ExtraLarge"
                                    }]
                                },
                                {
                                    "type": "Column",
                                    "width": "stretch",
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": f"{status}: Arquivos Parados",
                                            "weight": "Bolder",
                                            "size": "Large",
                                            "wrap": True
                                        },
                                        {
                                            "type": "TextBlock",
                                            "text": f"Detectado em {agora}",
                                            "isSubtle": True,
                                            "spacing": "None",
                                            "size": "Small"
                                        }
                                    ]
                                }
                            ]
                        }
                    ],
                    "bleed": True
                },
                
                # Informações do servidor
                {
                    "type": "Container",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "FactSet",
                            "facts": [
                                {
                                    "title": "Servidor:",
                                    "value": SSH_HOST
                                },
                                {
                                    "title": "Pasta:",
                                    "value": PASTA
                                },
                                {
                                    "title": "Total:",
                                    "value": f"{total} arquivo{'s' if total != 1 else ''}"
                                },
                                {
                                    "title": "Parado há:",
                                    "value": f"Mais de {MINUTOS} minuto{'s' if MINUTOS != 1 else ''}"
                                }
                            ]
                        }
                    ]
                },
                
                # Lista de arquivos
                {
                    "type": "Container",
                    "spacing": "Medium",
                    "separator": True,
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": f"Primeiros {min(LIMITE, total)} arquivos:",
                            "weight": "Bolder",
                            "size": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": "\n".join([f"• {arq}" for arq in arquivos_listados]),
                            "wrap": True,
                            "spacing": "Small",
                            "size": "Small"
                        }
                    ]
                }
            ]
        }
    }]
}

# Adiciona aviso se tiver mais arquivos
if total > LIMITE:
    payload["attachments"][0]["content"]["body"].append({
        "type": "Container",
        "spacing": "Small",
        "items": [{
            "type": "TextBlock",
            "text": f"E mais {total - LIMITE} arquivo{'s' if (total - LIMITE) != 1 else ''}...",
            "isSubtle": True,
            "weight": "Bolder",
            "size": "Small"
        }]
    })

# ===== ENVIA PARA O TEAMS =====
try:
    response = requests.post(TEAMS_URL, json=payload)
    if response.status_code == 202:
        print("✅ Mensagem enviada para Teams com sucesso!")
        registrar_envio()
    else:
        print(f"❌ Falha ao enviar: {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"❌ Erro ao enviar: {e}")