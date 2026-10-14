# - Supervisor Agent: chooses the task based on the user request.
# - ETL Agent: executes Bronze, Silver, and Gold.
# - Data Quality Agent: validates the Silver layer.
# - Analytics Agent: analyzes the Gold datasets.
# - Explanation Agent: answers conceptual questions without running ETL.

from typing import TypedDict, Any
from urllib.parse import urlparse

from langchain_openai import AzureChatOpenAI
from langgraph.graph import StateGraph, START, END

from config import foundry_endpoint, foundry_api_key, foundry_model
from etl_pipeline import run_etl
from quality_checks import run_quality_checks
from gold_analytics import get_gold_metrics


parsed = urlparse(foundry_endpoint)
azure_resource_url = f"{parsed.scheme}://{parsed.netloc}"

llm = AzureChatOpenAI(azure_endpoint=azure_resource_url,
                      azure_deployment=foundry_model,
                      api_key=foundry_api_key,
                      api_version="2024-12-01-preview",
                      temperature=1,
                      max_retries=2,
                      timeout=60)


class ETLState(TypedDict, total=False):
    query: str
    route: str
    etl_result: dict
    quality_result: dict
    analytics_result: dict
    answer: str


def build_agent_graph(spark):

    def supervisor_agent(state: ETLState):
        prompt = f"""
        You are the supervisor of a Databricks Uber Taxi ETL system.
        Choose exactly one route:
        run_etl
        quality
        analytics
        explain

        Rules:
        - run_etl: execute the complete Bronze, Silver, Gold pipeline.
        - quality: inspect the existing Silver Delta dataset.
        - analytics: analyze existing Gold summaries.
        - explain: answer a general conceptual question.

        User request: {state['query']}

        Return only the route name.
        """

        response = llm.invoke(prompt)
        route = response.content.strip().lower()

        allowed_routes = {
            "run_etl", "quality", "analytics", "explain"
        }

        if route not in allowed_routes:
            query = state["query"].lower()

            if any(x in query for x in ["run etl", "execute etl", "refresh pipeline", "build pipeline"]):
                route = "run_etl"
            elif any(x in query for x in ["quality", "null", "invalid", "duplicates"]):
                route = "quality"
            elif any(x in query for x in ["revenue", "payment", "location", "analytics", "trips"]):
                route = "analytics"
            else:
                route = "explain"

        return {"route": route}

    def etl_agent(state: ETLState):
        try:
            return {"etl_result": run_etl(spark)}
        except Exception as exc:
            return {
                "etl_result": {
                    "status": "failed",
                    "error": str(exc)
                }
            }

    def quality_agent(state: ETLState):
        try:
            return {
                "quality_result": run_quality_checks(spark)
            }
        except Exception as exc:
            return {
                "quality_result": {
                    "status": "error",
                    "error": str(exc)
                }
            }

    def analytics_agent(state: ETLState):
        try:
            metrics = get_gold_metrics(spark)
            response = llm.invoke(
                f"""You are a uber taxi business analytics agent.
                Answer the user's question using only these computed Gold metrics.
                Do not invent values. State that results are based on the available data.
                Metrics:
                {metrics}

                Question:
                {state['query']}
                """)
            return {
                "analytics_result": metrics,
                "answer": response.content
            }

        except Exception as exc:
            return {
                "analytics_result": {
                    "status": "error",
                    "error": str(exc)
                },
                "answer": f"Analytics failed: {exc}"}

    def explanation_agent(state: ETLState):
        response = llm.invoke(
            f"""Explain this Databricks ETL question clearly.
            Do not claim to have executed Spark jobs or inspected data.
            Question: {state['query']}
            """
        )
        return {"answer": response.content}

    def after_etl(state: ETLState):
        result = state.get("etl_result", {})
        return "quality" if result.get("status") == "success" else "end"

    def after_quality(state: ETLState):
        return ("analytics"
                if state.get("route") == "run_etl"
                else "end")

    builder = StateGraph(ETLState)
    builder.add_node("supervisor", supervisor_agent)
    builder.add_node("etl", etl_agent)
    builder.add_node("quality", quality_agent)
    builder.add_node("analytics", analytics_agent)
    builder.add_node("explain", explanation_agent)

    builder.add_edge(START, "supervisor")

    builder.add_conditional_edges(
        "supervisor",
        lambda state: state["route"],
        {
            "run_etl": "etl",
            "quality": "quality",
            "analytics": "analytics",
            "explain": "explain"
        }
    )

    builder.add_conditional_edges(
        "etl",
        after_etl,
        {
            "quality": "quality",
            "end": END
        }
    )

    builder.add_conditional_edges(
        "quality",
        after_quality,
        {
            "analytics": "analytics",
            "end": END
        }
    )

    builder.add_edge("analytics", END)
    builder.add_edge("explain", END)

    return builder.compile()