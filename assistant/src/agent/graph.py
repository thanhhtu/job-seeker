"""Job Seeker Agent - main LangGraph state machine."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Annotated, TypedDict

from langchain.messages import AnyMessage, HumanMessage, SystemMessage
from langchain_mistralai import ChatMistralAI
from langgraph.graph.message import add_messages

if TYPE_CHECKING:
    from langgraph.config import RunnableConfig
    from langgraph_api._factory_utils import ServerRuntime


# ─── Intent Recognition ───────────────────────────────────────────────────────

SYSTEM_PROMPT = """Bạn là trợ lý tìm kiếm việc làm bằng Tiếng Việt. Vai trò của bạn là giúp người dùng tìm việc từ cơ sở dữ liệu của chúng tôi.

<instructions>
- Trích xuất cụm từ tìm kiếm sạch từ tin nhắn của người dùng
- Loại bỏ các từ thừa như "tìm việc", "xin", "ở", "tại", "kiếm", "làm", "tuyển",..cetc
- Giữ lại các từ khóa quan trọng: chức danh, kỹ năng, tên công ty, địa điểm
- Nếu tin nhắn KHÔNG phải về tìm việc (chào hỏi, hỏi thông tin công ty, etc),
  trả về "NOT_SEARCH"
- Nếu tin nhắn không đủ thông tin để tìm kiếm (chỉ nói "chào", "tìm việc" mà không có gì khác),
  trả về "NEED_MORE_INFO"
</instructions>

<examples>
<example>
User: "tìm việc backend python"
Output: "backend python"
</example>
<example>
User: "tìm việc backend hà nội"
Output: "backend hà nội"
</example>
<example>
User: "tìm việc devops hà nội"
Output: "devops hà nội"
</example>
<example>
User: "Data Engineer"
Output: "data engineer"
</example>
<example>
User: "data engineer hcm"
Output: "data engineer ho_chi_minh"
</example>
<example>
User: "giới thiệu công ty VinSmart"
Output: "NOT_SEARCH"
</example>
<example>
User: "chào bạn"
Output: "NEED_MORE_INFO"
</example>
<example>
User: "tìm việc"
Output: "NEED_MORE_INFO"
</example>
<example>
User: "tuyển devops kubernetes docker"
Output: "devops kubernetes docker"
</example>
<example>
User: "Senior Python Developer"
Output: "senior python developer"
</example>
<example>
User: "Kỹ sư DevOps"
Output: "devops"
</example>
</examples>

<output_format>
- Trả về CHỉ text tìm kiếm, không có giải thích gì thêm
- NOT_SEARCH: "NOT_SEARCH"
- NEED_MORE_INFO: "NEED_MORE_INFO"
</output_format>
"""


async def extract_query(user_message: str, llm: ChatMistralAI) -> str | None:
    """Use LLM to extract clean search query from Vietnamese user message.

    Returns:
        - None: NOT_SEARCH (greeting, non-job question)
        - "NEED_MORE_INFO": vague message, need more details
        - str: clean search query
    """
    response = await llm.ainvoke(
        [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=f"<user_message>{user_message}</user_message>"),
        ]
    )
    query = response.content.strip()

    if query == "NOT_SEARCH" or not query:
        return None
    if query == "NEED_MORE_INFO":
        return "NEED_MORE_INFO"
    return query


# ─── Response Formatting ───────────────────────────────────────────────────────


def format_job_summary(job) -> str:
    """Format single job for display."""
    salary = _format_salary(job)
    location = ", ".join(job.locations) if job.locations else "Không rõ"
    return f"{job.title} tại {job.company_name} - {salary} - {location}"


def _format_salary(job) -> str:
    """Format salary for display."""
    if job.salary_negotiable:
        return "Thỏa thuận"
    if job.salary_min and job.salary_max:
        if job.salary_currency == "USD":
            return f"${job.salary_min:.0f}-{job.salary_max:.0f}"
        return f"{job.salary_min:.0f}-{job.salary_max:.0f} triệu"
    if job.salary_min:
        return f"Từ {job.salary_min:.0f} triệu"
    return "Không công khai"


# ─── State ────────────────────────────────────────────────────────────────────


class AgentState(TypedDict):
    """State for the job search agent graph."""

    messages: Annotated[list[AnyMessage], add_messages]
    query_text: str | None  # None=NOT_SEARCH, "NEED_MORE_INFO"=ask, else=query
    results: list | None
    pending_message: (
        str | None
    )  # message to append (used for cross-node message construction)


# ─── Node helpers ────────────────────────────────────────────────────────────

GREETING = "Xin chào! Tôi là trợ lý tìm việc. Bạn cần tôi giúp gì không?"
ASK_MORE = (
    "Để tìm việc phù hợp, bạn cho tôi biết thêm nhé:\n"
    "- Bạn muốn làm vị trí gì? (backend, frontend, devops, data...)\n"
    "- Bạn cần kỹ năng gì? (Python, Docker, Kubernetes...)\n"
    "- Bạn muốn làm ở đâu? (Hà Nội, TP HCM, remote...)"
)
NO_RESULTS = "Xin lỗi, hiện tại không có việc làm nào phù hợp với từ khóa bạn tìm kiếm. Bạn thử thay đổi từ khóa hoặc cho tôi biết thêm yêu cầu nhé!"


def _format_results(state: AgentState) -> str:
    """Build the results message string."""
    results = state.get("results", [])
    lines = [f"Tìm thấy {len(results)} việc làm phù hợp:\n"]
    for i, job in enumerate(results[:10], 1):
        salary = _format_salary(job)
        loc = _format_location(job)
        lines.append(f"{i}. {job.title}")
        lines.append(f"   Công ty: {job.company_name}")
        lines.append(f"   Mức lương: {salary}")
        lines.append(f"   Địa điểm: {loc}")
        lines.append(f"   Link: {job.url}")
        lines.append("")
    lines.append("Bạn có thể hỏi chi tiết thêm về việc #1, hoặc tìm với từ khóa khác.")
    return "\n".join(lines)


def _format_location(job) -> str:
    """Format location for display."""
    if job.locations:
        return ", ".join(job.locations)
    return "Không rõ"


# ─── Nodes ────────────────────────────────────────────────────────────────────


async def extract_node(state: AgentState, llm) -> dict:
    """Use LLM to extract search query from last user message."""
    messages = state.get("messages")
    if not messages or len(messages) == 0:
        return {"query_text": None}  # New conversation - ask for info
    last_msg = messages[-1]
    if isinstance(last_msg, dict):
        content = last_msg.get("content", "")
    else:
        content = last_msg.content

    print(f"[DEBUG] extract_node received content: {content!r}")

    query = await extract_query(content, llm)
    print(f"[DEBUG] extract_query returned: {query!r}")

    return {"query_text": query}


async def search_node(state: AgentState, repo, embedder) -> dict:
    """Execute 2-phase hybrid search: FTS + vector, fused via Python RRF."""
    query_text = state.get("query_text")
    if not query_text or query_text == "NEED_MORE_INFO":
        return {"results": None}

    print(f"[DEBUG] search_node received query_text: {query_text!r}")

    # Generate embedding for vector search phase
    embedding = None
    try:
        embedding_result = await embedder.embed_query(query_text)
        if embedding_result and hasattr(embedding_result, "embedding"):
            embedding = embedding_result.embedding
        elif isinstance(embedding_result, list):
            embedding = embedding_result
        print(f"[DEBUG] embedding generated: {embedding is not None}")
    except Exception as e:
        print(f"[DEBUG] embedding generation failed: {e}")

    try:
        results = await repo.search(query_text, embedding=embedding, limit=20)
    except Exception as e:
        print(f"[DEBUG] search failed: {e}")
        return {"results": None}

    print(f"[DEBUG] search returned {len(results)} results")
    return {"results": list(results)}


def greet_node(state: AgentState) -> dict:
    """Handle non-job messages (greetings, company questions, etc)."""
    return {"pending_message": GREETING}


def ask_more_node(state: AgentState) -> dict:
    """Handle vague queries that need more detail."""
    return {"pending_message": ASK_MORE}


async def format_results_node(state: AgentState) -> dict:
    """Format search results for display."""
    results = state.get("results")
    print(f"[DEBUG] format_results_node received results: {results}")
    if not results:
        return {"pending_message": NO_RESULTS}
    formatted = _format_results(state)
    print(f"[DEBUG] formatted message length: {len(formatted)}")
    return {"pending_message": formatted}


def append_message_node(state: AgentState) -> dict:
    """Append the constructed pending_message and end."""
    pending = state.get("pending_message")
    print(f"[DEBUG] append_message_node pending: {pending}")
    if pending:
        return {"messages": [HumanMessage(content=pending)], "pending_message": None}
    return {"pending_message": None}


# ─── Graph Factory ────────────────────────────────────────────────────────────


def graph(
    config: "ServerRuntime | RunnableConfig | None" = None,
    checkpoint_saver: "RunnableConfig | None" = None,
):
    """Build and return the compiled job search agent graph.

    This is the entry point referenced by langgraph.json.

    LangGraph API calls this with ServerRuntime and/or RunnableConfig.
    We create all dependencies internally.

    Args:
        config: ServerRuntime or RunnableConfig (unused, for LangGraph API compatibility)
        checkpoint_saver: Optional checkpoint saver (unused, for LangGraph API)

    Returns:
        Compiled LangGraph state machine
    """
    from langgraph.graph import END, StateGraph

    from agent.db.repository import JobRepository

    llm = ChatMistralAI(model="mistral-large-latest")
    repo = JobRepository()

    # Lazy embedder - initialized in a thread to avoid blocking the event loop
    _embedder = None

    def _create_embedder():
        from langchain_mistralai import MistralAIEmbeddings
        return MistralAIEmbeddings(model="mistral-embed")

    async def extract_wrapper(state):
        return await extract_node(state, llm)

    async def search_wrapper(state):
        nonlocal _embedder
        if _embedder is None:
            _embedder = await asyncio.to_thread(_create_embedder)
        return await search_node(state, repo, _embedder)

    compiled = StateGraph(AgentState)
    compiled.add_node("extract", extract_wrapper)
    compiled.add_node("search", search_wrapper)
    compiled.add_node("ask_more", ask_more_node)
    compiled.add_node("format_results", format_results_node)
    compiled.add_node("append_message", append_message_node)
    compiled.set_entry_point("extract")

    def route_extract(s: AgentState) -> str:
        query_text = s.get("query_text")
        if query_text is None or query_text == "" or query_text == "NEED_MORE_INFO":
            return "ask_more"
        return "search"

    def route_search(s: AgentState) -> str:
        query_text = s.get("query_text")
        if query_text == "NEED_MORE_INFO":
            return "ask_more"
        return "format_results"

    compiled.add_conditional_edges("extract", route_extract)
    compiled.add_conditional_edges("search", route_search)
    compiled.add_edge("ask_more", "append_message")
    compiled.add_edge("format_results", "append_message")
    compiled.add_edge("append_message", END)

    return compiled.compile()


__all__ = [
    "AgentState",
    "graph",
    "extract_query",
    "format_job_summary",
    "SYSTEM_PROMPT",
]
