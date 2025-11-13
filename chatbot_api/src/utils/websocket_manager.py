from typing import List
from fastapi import WebSocket
import json
import logging

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Gerencia múltiplas conexões WebSocket para broadcast de alertas."""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []

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

    async def send_personal(self, websocket: WebSocket, message: dict):
        """Envia uma mensagem para um cliente específico."""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem pessoal: {e}")
            self.disconnect(websocket)
