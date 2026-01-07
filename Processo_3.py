# - Processo 3
# 1. Conecta na pasta de rede (172.20.1.43)
# 2. Monitora a pasta A:\SANTANDER\retorno\BKP
# 3. Valida se recebeu o arquivo do dia (EXDDMMYY)

import os
import sys
import time
import platform
import subprocess
import threading
from pathlib import Path
from datetime import datetime, date, timedelta
from logging.handlers import RotatingFileHandler
import logging
import requests
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
    sys.exit(1)


class AutomacaoProcesso3:
    def __init__(self):

        self.folder_path = r"\\172.20.1.43\C\SANTANDER\retorno\BKP"
        

        self.prefix = "EX"
        

        self.check_mode = "both"  
        self.time_window_seconds = 600  
        self.include_subfolders = False
        

        self.run_schedules = ["07:40", "11:40", "15:40", "19:40"]
        self.minutes_lag = 9  
        
        self.max_files_to_scan = 20000
        self.scan_max_seconds = 20
        self.log_progress_every = 500
        

        self.pasta_logs = os.getenv('PASTA_LOGS', script_dir) or script_dir
        

        self.teams_webhook_url = os.getenv('TEAMS_WEBHOOK_URL')
        

        self.request_timeout = 15
        

        self.log_max_bytes = 3 * 1024 * 1024
        self.log_backup_count = 3
        

        self._validar_variaveis()
        

        self.logger = self._setup_logger()
    
    def _validar_variaveis(self):
        """Valida se todas as variáveis necessárias foram carregadas"""
        variaveis_obrigatorias = {
            'PASTA_LOGS': self.pasta_logs,
            'TEAMS_WEBHOOK_URL': self.teams_webhook_url
        }
        
        faltando = [var for var, valor in variaveis_obrigatorias.items() if not valor]
        
        if faltando:
            raise ValueError(f"❌ Variáveis faltando no arquivo .env: {', '.join(faltando)}")
        
        print("✅ Todas as variáveis de ambiente carregadas com sucesso!")
    
    def _setup_logger(self):
        """Configura o sistema de logging"""
        try:
            os.makedirs(self.pasta_logs, exist_ok=True)
        except Exception as e:
            print(f"⚠️ Não foi possível criar PASTA_LOGS='{self.pasta_logs}': {e}")
            self.pasta_logs = script_dir
        
        log_path = os.path.join(self.pasta_logs, "validador_arquivo_santander.log")
        
        logger = logging.getLogger("validador_arquivo_santander")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
        

        fh = RotatingFileHandler(
            log_path, 
            maxBytes=self.log_max_bytes, 
            backupCount=self.log_backup_count, 
            encoding="utf-8"
        )
        fh.setFormatter(fmt)
        

        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def parse_hhmm_on(self, day: date, hhmm: str) -> datetime:
        """Converte string HH:MM em datetime para um dia específico"""
        h, m = map(int, hhmm.split(":"))
        return datetime(day.year, day.month, day.day, h, m, 0)
    
    def previous_run_schedule(self, now: datetime) -> tuple:
        """
        Retorna (run_time_escolhido, expected_time = run - MINUTES_LAG)
        - Escolhe a ÚLTIMA janela do dia que seja <= now
        - Se nenhuma, pega a última janela de ontem
        """
        today = now.date()
        todays = [self.parse_hhmm_on(today, s) for s in self.run_schedules]
        past = [dt for dt in sorted(todays) if dt <= now]
        
        if past:
            run_dt = past[-1]
        else:
            yday = today - timedelta(days=1)
            ylist = [self.parse_hhmm_on(yday, s) for s in self.run_schedules]
            run_dt = sorted(ylist)[-1]
        
        expected_dt = run_dt - timedelta(minutes=self.minutes_lag)
        return run_dt, expected_dt
    
    def within_time_window(self, ts: float, expected_dt: datetime) -> bool:
        """Verifica se timestamp está dentro da janela de tolerância"""
        dt = datetime.fromtimestamp(ts)
        half = timedelta(seconds=self.time_window_seconds / 2)
        return (expected_dt - half) <= dt <= (expected_dt + half)
    
    def today_token_for(self, expected_dt: datetime) -> str:
        """Retorna DDMMYY de acordo com a data da janela esperada"""
        token = expected_dt.strftime("%d%m%y")
        self.logger.info(f"🔍 Token gerado para validação: {self.prefix}{token} (baseado em {expected_dt.strftime('%d/%m/%Y')})")
        return token
    
    def name_is_today_EX(self, filename: str, expected_dt: datetime) -> bool:
        """
        Verifica se o nome começa com EX + DDMMYY (case-insensitive)
        Ex.: EX061125, EX061125.csv, EX061125_retorno.txt
        """
        token = self.today_token_for(expected_dt)
        pattern = (self.prefix + token).upper()
        match = filename.upper().startswith(pattern)
        
        if match:
            self.logger.info(f"✅ Arquivo {filename} corresponde ao padrão {self.prefix}{token}")
        
        return match
    
    def extract_host_from_unc(self, unc_path: str) -> str:
        """Extrai o host de um caminho UNC. Ex.: \\172.20.1.43\C\share -> 172.20.1.43"""
        if not unc_path:
            return None
        
        p = unc_path.replace("/", "\\")
        if p.startswith("\\\\"):
            resto = p[2:]
            partes = resto.split("\\")
            if partes:
                return partes[0]
        return None
    
    def ping_host(self, host: str, timeout_ms: int = 1000) -> bool:
        """
        Verifica conectividade com ping
        - Windows: ping -n 1 -w <ms>
        - Linux/Mac: ping -c 1 -W <segundos>
        """
        if not host:
            return False
        
        try:
            if platform.system().lower().startswith("win"):
                cmd = ["ping", "-n", "1", "-w", str(timeout_ms), host]
            else:
                sec = max(1, timeout_ms // 1000)
                cmd = ["ping", "-c", "1", "-W", str(sec), host]
            
            res = subprocess.run(
                cmd, 
                stdout=subprocess.DEVNULL, 
                stderr=subprocess.DEVNULL, 
                timeout=(timeout_ms/1000)+1
            )
            return res.returncode == 0
        except Exception:
            return False
    
    def iter_files_with_limits(self, folder: Path, pattern_prefix: str):
        """Itera sobre arquivos com limites de tempo e quantidade"""
        start = time.time()
        seen = 0
        
        def check_limits():
            elapsed = time.time() - start
            if self.scan_max_seconds and elapsed > self.scan_max_seconds:
                raise TimeoutError(f"Varredura excedeu {self.scan_max_seconds}s")
            if self.max_files_to_scan and seen >= self.max_files_to_scan:
                raise TimeoutError(f"Varredura excedeu limite de {self.max_files_to_scan} arquivos")
        
        def scan_dir(dir_path: Path):
            nonlocal seen
            try:
                with os.scandir(dir_path) as it:
                    for entry in it:
                        check_limits()
                        
                        if entry.is_dir(follow_symlinks=False):
                            if self.include_subfolders:
                                yield from scan_dir(Path(entry.path))
                            continue
                        
                        if entry.is_file(follow_symlinks=False):
                            if entry.name.upper().startswith(pattern_prefix.upper()):
                                seen += 1
                                if seen % self.log_progress_every == 0:
                                    self.logger.info(
                                        f"Progresso: {seen} arquivos inspecionados "
                                        f"em {int(time.time()-start)}s"
                                    )
                                yield Path(entry.path)
            except PermissionError:
                self.logger.warning(f"Sem permissão em: {dir_path}")
            except FileNotFoundError:
                pass
            except OSError as e:
                self.logger.warning(f"Falha ao ler dir {dir_path}: {e}")
        
        yield from scan_dir(folder)
    
    def find_matches(self, expected_dt: datetime) -> tuple:
        """Busca arquivos que correspondem aos critérios"""
        folder = Path(self.folder_path)
        
        if not folder.exists():
            raise FileNotFoundError(f"Pasta não encontrada: {self.folder_path}")
        
        start_scan = time.time()
        found = []
        out_of_window = []
        total_seen = 0
        
        for f in self.iter_files_with_limits(folder, self.prefix):
            total_seen += 1
            
            try:
                st = f.stat()
            except Exception:
                continue
            
            by_name = self.name_is_today_EX(f.name, expected_dt) if self.check_mode in ("filename", "both") else False
            by_mtime = self.within_time_window(st.st_mtime, expected_dt) if self.check_mode in ("mtime", "both") else False
            
            if self.check_mode == "both":
                if by_name and by_mtime:
                    found.append(f)
                elif by_name and not by_mtime:
                    out_of_window.append(f)
            elif self.check_mode == "filename":
                if by_name:
                    found.append(f)
            elif self.check_mode == "mtime":
                if by_mtime:
                    found.append(f)
        
        elapsed = time.time() - start_scan
        self.logger.info(
            f"Varredura concluída: {total_seen} arquivos inspecionados em {elapsed:.1f}s "
            f"Encontrados: {len(found)} Fora da janela: {len(out_of_window)}"
        )
        
        return found, out_of_window
    
    def run_with_timeout(self, func, args=(), kwargs=None, timeout_sec: int = 20):
        """Executa função com timeout externo usando thread"""
        if kwargs is None:
            kwargs = {}
        
        result = {"ok": None, "val": None, "err": None}
        
        def _target():
            try:
                result["val"] = func(*args, **kwargs)
                result["ok"] = True
            except BaseException as e:
                result["err"] = e
                result["ok"] = False
        
        th = threading.Thread(target=_target, daemon=True)
        th.start()
        th.join(timeout_sec)
        
        if th.is_alive():
            return (False, None, None)
        
        if result["err"] is not None:
            raise result["err"]
        
        return (True, result["val"], None)
    
    def enviar_para_teams(self, titulo: str, subtitulo_markdown: str, 
                         facts: list, status_geral: str, container_style: str):
        """Envia mensagem para o Teams via Adaptive Card"""
        try:
            timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            
            facts_adaptive = [{"title": k, "value": v} for (k, v) in facts]
            
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
                                "text": f"{titulo} - {status_geral}"
                            },
                            {
                                "type": "TextBlock",
                                "isSubtle": True,
                                "wrap": True,
                                "spacing": "None",
                                "text": f"**Execução:** {timestamp}\n{subtitulo_markdown}"
                            },
                            {
                                "type": "Container",
                                "style": container_style,
                                "items": [
                                    {"type": "FactSet", "facts": facts_adaptive}
                                ]
                            }
                        ]
                    }
                }]
            }
            
            response = requests.post(
                self.teams_webhook_url, 
                json=adaptive_payload, 
                timeout=self.request_timeout
            )
            
            if response.status_code == 202:
                self.logger.info("✅ Mensagem enviada para o Teams com sucesso!")
                return True
            else:
                self.logger.error(f"❌ Erro ao enviar para o Teams: {response.status_code}")
                self.logger.error(f"Resposta: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Erro ao enviar mensagem para o Teams: {e}")
            return False
    
    def executar(self):
        """Executa o processo completo de verificação"""
        self.logger.info("=" * 80)
        self.logger.info("INICIANDO PROCESSO 3 - VERIFICAÇÃO DE ARQUIVO SANTANDER")
        self.logger.info("=" * 80)
        
        now = datetime.now()
        self.logger.info(f"Pasta alvo: {self.folder_path}")
        self.logger.info(f"Modo: {self.check_mode} | Tolerância mtime: {self.time_window_seconds}s")
        

        try:
            run_dt, expected_dt = self.previous_run_schedule(now)
            arquivo_esperado = f"{self.prefix}{expected_dt.strftime('%d%m%y')}"
            self.logger.info(
                f"Janela selecionada: execução de {run_dt.strftime('%Y-%m-%d %H:%M')} | "
                f"Arquivo esperado: {arquivo_esperado}* (mtime: {expected_dt.strftime('%Y-%m-%d %H:%M')})"
            )
        except Exception as e:
            self.logger.exception(f"Falha ao calcular janela: {e}")
            
            self.enviar_para_teams(
                titulo="📁 Verificação EX",
                subtitulo_markdown=f"Erro ao calcular janela\nPasta: `{self.folder_path}`",
                facts=[("Erro", str(e))],
                status_geral="❗Erro",
                container_style="attention"
            )
            
            return False
        

        host_unc = self.extract_host_from_unc(self.folder_path)
        if host_unc and not self.ping_host(host_unc, timeout_ms=1000):
            msg = f"Servidor inacessível: {host_unc}"
            self.logger.error(msg)
            
            self.enviar_para_teams(
                titulo="📁 Verificação EX",
                subtitulo_markdown=f"Pasta: `{self.folder_path}`",
                facts=[
                    ("Host", host_unc),
                    ("Status", "Sem resposta ao ping (1s)")
                ],
                status_geral="❌ Falha de conectividade",
                container_style="attention"
            )
            
            return False
        

        try:
            self.logger.info("Iniciando varredura da pasta de rede...")
            
            ok_timeout, res, _ = self.run_with_timeout(
                self.find_matches,
                args=(expected_dt,),
                timeout_sec=self.scan_max_seconds + 10
            )
            
            if not ok_timeout:
                msg = f"Acesso à pasta levou mais de {self.scan_max_seconds + 10}s (possível bloqueio)"
                self.logger.error(msg)
                
                self.enviar_para_teams(
                    titulo="📁 Monitoramento Arquivo Santander",
                    subtitulo_markdown=f"Pasta: `{self.folder_path}`",
                    facts=[
                        ("Host", host_unc or "-"),
                        ("Timeout externo", f"{self.scan_max_seconds + 10}s")
                    ],
                    status_geral="⚠️ Timeout no acesso à pasta",
                    container_style="warning"
                )
                
                return False
            
            matches, out_of_window = res
            token = self.today_token_for(expected_dt)
            

            if matches:
                matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                
                facts = [
                    ("Pasta", self.folder_path),
                    ("Execução considerada", run_dt.strftime("%Y-%m-%d %H:%M")),
                    ("Total encontrados", str(len(matches)))
                ]
                

                for p in matches[:5]:
                    st = p.stat()
                    facts.append((
                        f"• {p.name}",
                        f"mtime {datetime.fromtimestamp(st.st_mtime).strftime('%H:%M:%S')} — {st.st_size} bytes"
                    ))
                
                self.enviar_para_teams(
                    titulo=f"📁 Monitoramento Arquivo Santander",
                    subtitulo_markdown=f"Pasta: `{self.folder_path}`",
                    facts=facts,
                    status_geral="✅ Arquivo(s) encontrado(s)",
                    container_style="good"
                )
                
                self.logger.info("=" * 80)
                self.logger.info("PROCESSO 3 CONCLUÍDO COM SUCESSO - ARQUIVOS ENCONTRADOS!")
                self.logger.info("=" * 80)
                
                return True
            
            else:
                facts = [
                    ("Pasta", self.folder_path),
                    ("Execução considerada", run_dt.strftime("%Y-%m-%d %H:%M")),
                    ("Encontrados fora da janela", str(len(out_of_window)))
                ]
                

                if out_of_window:
                    out_of_window.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                    for p in out_of_window[:5]:
                        st = p.stat()
                        facts.append((
                            f"• {p.name}",
                            f"FORA janela — mtime {datetime.fromtimestamp(st.st_mtime).strftime('%H:%M:%S')} — {st.st_size} bytes"
                        ))
                
                self.enviar_para_teams(
                    titulo=f"📁 Monitoramento Arquivo Santander",
                    subtitulo_markdown=f"Pasta: `{self.folder_path}`",
                    facts=facts,
                    status_geral="❌ Arquivo NÃO recebido na janela",
                    container_style="attention"
                )
                
                self.logger.warning("=" * 80)
                self.logger.warning("PROCESSO 3 CONCLUÍDO - ARQUIVO NÃO ENCONTRADO!")
                self.logger.warning("=" * 80)
                
                return False
        
        except TimeoutError as te:
            self.logger.error(f"Tempo limite na varredura: {te}")
            
            facts = [
                ("Pasta", self.folder_path),
                ("Execução considerada", run_dt.strftime("%Y-%m-%d %H:%M")),
                ("Limites", f"{self.max_files_to_scan} arquivos / {self.scan_max_seconds}s")
            ]
            
            self.enviar_para_teams(
                titulo="📁 Monitoramento Arquivo Santander",
                subtitulo_markdown=f"Pasta: `{self.folder_path}`",
                facts=facts,
                status_geral="⚠️ Varredura interrompida por tempo limite",
                container_style="warning"
            )
            
            return False
        
        except Exception as e:
            self.logger.exception(f"Erro na verificação: {e}")
            
            self.enviar_para_teams(
                titulo="📁 Verificação EX",
                subtitulo_markdown=f"Pasta: `{self.folder_path}`",
                facts=[("Erro", str(e))],
                status_geral="❗Erro na execução do verificador",
                container_style="attention"
            )
            
            return False



if __name__ == "__main__":
    processo3 = AutomacaoProcesso3()
    sucesso = processo3.executar()
    sys.exit(0 if sucesso else 1)