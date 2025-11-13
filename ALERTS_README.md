# Sistema de Alertas em Tempo Real - Ransomware CTI Chatbot

## 📋 O que foi implementado

Um **sistema de alertas em tempo real** que permite que você envie notificações de qualquer lugar e o frontend (Streamlit) as receba automaticamente, exibindo-as no chat.

### Componentes:

1. **Backend (FastAPI)**
   - `WebSocketManager` (`src/utils/websocket_manager.py`): Gerencia múltiplas conexões WebSocket
   - Novo endpoint `/ws/alerts`: WebSocket para conectar clientes
   - Novo endpoint `/alert` (POST): Dispara alertas para todos os clientes conectados

2. **Frontend (Streamlit)**
   - Listener de alertas em thread separada que se conecta ao WebSocket
   - Exibe alertas recebidos na interface com cores de severidade
   - Adiciona alertas ao histórico de chat

3. **Script de Teste**
   - `test_alerts.py`: Script para disparar alertas manualmente

## 🚀 Como usar

### 1. Instalar dependências

```bash
# Backend
pip install websockets

# Frontend
pip install websockets
```

Ou adicione manualmente aos `requirements.txt` / `pyproject.toml` (já feito nos arquivos):
- `websockets>=12.0`

### 2. Iniciar os serviços

```bash
# Terminal 1: Backend
cd chatbot_api
python -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

# Terminal 2: Frontend
cd chatbot_frontend
streamlit run src/main.py

# Terminal 3 (opcional): Docker Compose
docker-compose up
```

### 3. Disparar alertas

#### Opção A: Usar o script de teste
```bash
python test_alerts.py
```

#### Opção B: Usar curl ou Postman
```bash
curl -X POST http://localhost:8000/alert \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Nova Ameaça",
    "message": "Grupo XYZ detectado",
    "severity": "warning"
  }'
```

#### Opção C: Programaticamente (Python)
```python
import requests
from datetime import datetime

requests.post(
    "http://localhost:8000/alert",
    json={
        "title": "Alerta Crítico",
        "message": "Falha na análise de ransomware",
        "severity": "error",
        "timestamp": datetime.now().isoformat()
    }
)
```

## 📊 Estrutura do Alerta

Cada alerta possui:

```json
{
  "type": "alert",
  "title": "Título do Alerta",
  "message": "Descrição detalhada",
  "severity": "info|warning|error|success",
  "timestamp": "2025-11-13T10:30:00.000Z"
}
```

**Severidades:**
- `info` (azul ℹ️): Informações gerais
- `warning` (amarelo ⚠️): Avisos importantes
- `error` (vermelho 🚨): Erros críticos
- `success` (verde ✅): Operações bem-sucedidas

## 🔧 Customização

### Adicionar novos tipos de alerta

No `chatbot_api/src/main.py`:

```python
@app.post("/alert/threat")
async def send_threat_alert(threat_data: dict):
    """Alerta específico para ameaças."""
    alert = {
        "type": "threat",
        "actor": threat_data.get("actor"),
        "family": threat_data.get("family"),
        "victims_count": threat_data.get("victims_count"),
        "severity": "warning"
    }
    await manager.broadcast(alert)
    return {"status": "Threat alert sent"}
```

### Integrar com seu sistema de ameaças

Você pode chamar o endpoint `/alert` de:
- Scripts de análise automatizada
- Webhooks de ferramentas externas
- Agentes de IA/ML
- Sistemas de detecção de anomalias

Exemplo integrando com seu RAG agent:

```python
# No seu agente, quando detectar algo importante:
async def rag_agent_executor_with_alerts(query: str):
    response = await invoke_agent_with_retry(query)
    
    # Se a resposta contém algo crítico, dispara alerta
    if "crítico" in response.get("output", "").lower():
        await manager.broadcast({
            "type": "alert",
            "title": "Resultado Crítico",
            "message": response["output"][:200],
            "severity": "warning"
        })
    
    return response
```

## 🧪 Testando

1. Abra o Streamlit: `http://localhost:8501`
2. Veja na sidebar "Status de Alertas" mostrando "✅ Conectado aos alertas!"
3. Em outro terminal, execute:
   ```bash
   python test_alerts.py
   ```
4. Os alertas aparecerão em tempo real no chat!

## ⚠️ Notas Importantes

- **CORS habilitado**: O backend permite conexões de qualquer origem (use `allow_origins=["http://localhost:3000"]` em produção)
- **Persistência**: Alertas não são persistidos. Para guardar, adicione um banco de dados
- **Autenticação**: Sem autenticação. Adicione JWT/OAuth para produção
- **Reconexão**: Se desconectar, a página faz refresh automático

## 📝 Próximos passos

1. Adicionar persistência de alertas em banco de dados
2. Integrar com seu sistema de detecção de ameaças
3. Criar dashboard de histórico de alertas
4. Implementar filtros por severidade/tipo
5. Adicionar autenticação WebSocket

---

Qualquer dúvida, consulte o código comentado em:
- `chatbot_api/src/main.py`
- `chatbot_api/src/utils/websocket_manager.py`
- `chatbot_frontend/src/main.py`
