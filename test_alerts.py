#!/usr/bin/env python3
"""
Script para testar o sistema de alertas.
Dispara alertas para o frontend Streamlit.

Uso:
    python test_alerts.py
"""

import requests
import json
from datetime import datetime

# Configure a URL da API
API_URL = "http://localhost:8000"

def send_alert(title: str, message: str, severity: str = "info"):
    """Envia um alerta para o sistema."""
    try:
        response = requests.post(
            "http://localhost:8000/alert",
            json={
                "title": title,
                "message": message,
                "severity": severity,
                "timestamp": datetime.now().isoformat(),
            }
        )
        
        if response.status_code == 200:
            print(f"✅ Alerta enviado: {title}")
            print(f"   Resposta: {response.json()}")
        else:
            print(f"❌ Erro ao enviar alerta: {response.status_code}")
            print(f"   {response.text}")
    except requests.exceptions.ConnectionError:
        print(f"❌ Não foi possível conectar à API em {API_URL}")
        print("   Certifique-se de que o backend está rodando.")
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    print("=== Teste de Sistema de Alertas ===\n")
    
    # Teste 1: Alerta de informação
    print("1. Enviando alerta de informação...")
    send_alert(
        "Nova Ameaça Detectada",
        "Um novo ator de ameaça foi identificado: Grupo XYZ",
        "info"
    )
    
    print("\n2. Enviando alerta de aviso...")
    # Teste 2: Alerta de aviso
    send_alert(
        "Atividade Suspeita",
        "Spike de tráfego de ransomware LockBit detectado",
        "warning"
    )
    
    print("\n3. Enviando alerta de erro...")
    # Teste 3: Alerta de erro
    send_alert(
        "Falha Crítica",
        "Falha na conexão com o grafo de conhecimento",
        "error"
    )
    
    print("\n4. Enviando alerta de sucesso...")
    # Teste 4: Alerta de sucesso
    send_alert(
        "Análise Concluída",
        "Análise de 1000 amostras completada com sucesso",
        "success"
    )
    
    print("\n✨ Testes concluídos!")
