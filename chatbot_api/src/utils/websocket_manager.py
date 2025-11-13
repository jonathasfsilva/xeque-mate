from typing import List
from collections import deque
from datetime import datetime
from fastapi import WebSocket
import json
import logging

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Gerencia múltiplas conexões WebSocket para broadcast de alertas."""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        # buffer with recent alerts for polling clients
        self.recent_alerts = deque(maxlen=200)

    async def connect(self, websocket: WebSocket):
        """Aceita uma conexão WebSocket."""
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Client conectado. Total de conexões: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        """Remove uma conexão WebSocket."""
        self.active_connections.remove(websocket)
        logger.info(f"Client desconectado. Total de conexões: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Envia uma mensagem para todos os clientes conectados."""
        disconnected = []
        
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.warning(f"Erro ao enviar mensagem: {e}")
                disconnected.append(connection)
        
        # Remove conexões que falharam
        for conn in disconnected:
            self.disconnect(conn)

    def add_alert(self, alert: dict):
        """Store an alert in the recent buffer (timestamp is added if missing)."""
        if "timestamp" not in alert or not alert.get("timestamp"):
            alert["timestamp"] = datetime.utcnow().isoformat()
        # keep a shallow copy
        self.recent_alerts.appendleft(dict(alert))

    def get_recent_alerts(self, limit: int = 50, since: str | None = None):
        """Return up to `limit` recent alerts. If `since` is provided, return alerts with timestamp > since."""
        results = []
        for a in list(self.recent_alerts):
            if since:
                try:
                    if a.get("timestamp") and a["timestamp"] <= since:
                        continue
                except Exception:
                    pass
            results.append(a)
            if len(results) >= limit:
                break
        return results

    async def send_personal(self, websocket: WebSocket, message: dict):
        """Envia uma mensagem para um cliente específico."""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem pessoal: {e}")
            self.disconnect(websocket)
