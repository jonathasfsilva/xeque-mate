from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from src.agents.rag_agent import rag_agent_executor
from src.models.rag_query import QueryInput, QueryOutput
from src.utils.async_utils import async_retry
from src.utils.websocket_manager import ConnectionManager
import logging

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Ransomware Chatbot",
    description="Endpoints for a system graph RAG chatbot",
)

# Configurar CORS para permitir conexões do frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Gerenciador de conexões WebSocket
manager = ConnectionManager()


@async_retry(max_retries=10, delay=1)
async def invoke_agent_with_retry(query: str):
    """
    Retry the agent if a tool fails to run. This can help when there
    are intermittent connection issues to external APIs.
    """

    return await rag_agent_executor.ainvoke({"input": query})


@app.get("/")
async def get_status():
    return {"status": "running"}


@app.post("/rag-agent")
async def ask_hospital_agent(query: QueryInput) -> QueryOutput:
    query_response = await invoke_agent_with_retry(query.text)
    query_response["intermediate_steps"] = [
        str(s) for s in query_response["intermediate_steps"]
    ]

    return query_response


@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """Conecta um cliente ao stream de alertas em tempo real."""
    await manager.connect(websocket)
    try:
        while True:
            # Mantém a conexão aberta e aguarda mensagens
            data = await websocket.receive_text()
            logger.info(f"Mensagem recebida: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("Client desconectado do WebSocket")
    except Exception as e:
        logger.error(f"Erro no WebSocket: {e}")
        manager.disconnect(websocket)


@app.post("/alert")
async def send_alert(alert_data: dict):
    """Dispara um alerta para todos os clientes conectados."""
    alert_message = {
        "type": "alert",
        "title": alert_data.get("title", "Novo Alerta"),
        "message": alert_data.get("message", ""),
        "severity": alert_data.get("severity", "info"),  # info, warning, error, success
        "timestamp": alert_data.get("timestamp", ""),
    }
    # store in recent buffer and broadcast
    manager.add_alert(alert_message)
    await manager.broadcast(alert_message)
    logger.info(f"Alerta disparado: {alert_message}")
    
    return {"status": "Alert sent to all connected clients", "alert": alert_message}


@app.get("/alerts/recent")
async def get_recent_alerts(limit: int = 20, since: str | None = None):
    """Return recent alerts for polling clients."""
    alerts = manager.get_recent_alerts(limit=limit, since=since)
    return {"alerts": alerts}
