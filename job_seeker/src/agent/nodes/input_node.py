from src.agent.state import JobSearchState

def input_node(state: JobSearchState) -> dict:
    raw_query = state.get("raw_query", "").strip()

    if not raw_query:
        messages = state.get("messages", [])
        if messages:
            last_message = messages[-1]
            if isinstance(last_message, dict):
                raw_query = last_message.get("content", "").strip()
            else:
                raw_query = getattr(last_message, "content", "").strip()

    return {"raw_query": raw_query}
