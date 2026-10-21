"""
Verify Live Vertex AI Memory Bank & AWS Aurora Memory Bank Entries.
Run:
    python3 tests/verify_memory_bank.py
"""

import asyncio
import json
import os
from google.adk.memory.vertex_ai_memory_bank_service import VertexAiMemoryBankService

GCP_PROJECT = os.getenv("GCP_PROJECT", "vertexai-l300-capstone")
GCP_REGION = os.getenv("GCP_REGION", "us-central1")
ACT1_ENGINE_ID = os.getenv("ACT1_REASONING_ENGINE_ID", "4791592783888515072")


async def inspect_memory_bank():
    print("======================================================================")
    print(f"INSPECTING LIVE VERTEX AI MEMORY BANK (Engine ID: {ACT1_ENGINE_ID})")
    print("======================================================================")

    mb_service = VertexAiMemoryBankService(
        project=GCP_PROJECT,
        location=GCP_REGION,
        agent_engine_id=ACT1_ENGINE_ID,
    )

    resp = await mb_service.search_memory(
        app_name="gcp_control_plane",
        user_id="OMF-4821",
        query="OMF-4821 OMFRN9238 Dhanshri More 2020 Toyota Tacoma TILA 756 OH-8829104",
    )

    memories = getattr(resp, "memories", []) if resp else []
    print(f"\nFound {len(memories)} memory entries in Vertex AI Memory Bank for user_id='OMF-4821':\n")
    for idx, mem in enumerate(memories, start=1):
        text_parts = []
        if getattr(mem, "content", None) and getattr(mem.content, "parts", None):
            for p in mem.content.parts:
                if getattr(p, "text", None):
                    text_parts.append(p.text)
        print(f"--- Memory Entry #{idx} ---")
        for t in text_parts:
            print(f"  {t}")
        print()

    print("======================================================================")
    print("GCP Console URL to View Agent Runtime & Memory Bank:")
    print(
        f"https://console.cloud.google.com/vertex-ai/agents/agent-engines/locations/{GCP_REGION}/agent-engines/{ACT1_ENGINE_ID}?project={GCP_PROJECT}"
    )
    print("======================================================================")


if __name__ == "__main__":
    asyncio.run(inspect_memory_bank())
