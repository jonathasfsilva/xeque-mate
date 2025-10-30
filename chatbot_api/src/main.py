from fastapi import FastAPI
from src.agents.rag_agent import rag_agent_executor
from src.models.rag_query import QueryInput, QueryOutput
from src.utils.async_utils import async_retry

app = FastAPI(
    title="Ransomware Chatbot",
    description="Endpoints for a system graph RAG chatbot",
)


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
