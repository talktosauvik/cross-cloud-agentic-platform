# Cross-Cloud Agentic Platform

Enterprise agentic architecture demonstrating a **"Bridge to Locality"** across **Google Cloud Platform (GCP)** and **Amazon Web Services (AWS)** with zero sensitive data migration.

## Architecture Highlights
- **Front-Office / Sensory Edge**: Google Enterprise Conversational AI (GECX) Avatar streaming voice & dynamic UI cards.
- **Reasoning Engine**: Agent Development Kit (ADK 2.0) Graph Workflow state machine orchestrating deterministic underwriting, multimodal title OCR (Gemini 3.8 Flash), and TILA disclosures.
- **Enterprise Control Plane**: Gemini Enterprise Agent Platform (GEAP) with Agent Gateway, Model Armor security policies, Agent Registry, and Vertex AI Memory Bank for cross-session persistence.
- **Data Locality Boundary**: AWS FastMCP Server governing Amazon Aurora PostgreSQL and Amazon S3 with 3-Legged Delegated OAuth 2.0 + PKCE.

## Project Structure
```text
cross-cloud-agentic-platform/
├── aws_data_plane/
│   ├── database/
│   │   ├── schema.sql              # Aurora PostgreSQL DDL schema
│   │   └── seed_mock_data.sql      # Seed borrower, accounts, and collateral
│   ├── mcp_server/
│   │   ├── server.py               # FastMCP server with governed tools
│   │   └── Dockerfile              # Container spec for ECS Fargate
│   └── docs/
│       └── aws_setup_guide.md      # Argolis-compliant AWS deployment steps
├── gcp_control_plane/
│   ├── agent_workflow.py           # ADK 2.0 Graph Workflow engine
│   ├── memory_service.py           # Vertex AI Memory Bank integration
│   ├── title_ocr_agent.py          # Multimodal title extraction agent
│   └── models.py                   # Pydantic schemas (Title, Underwriting, Cards)
├── gateway_policies/
│   ├── gateway_config.yaml         # Agent Gateway & Model Armor config
│   └── registry_fastmcp.json       # MCP Agent Registry descriptor
├── tests/
│   └── eval/
│       ├── eval_config.yaml        # Evaluation rules (Trajectory, Memory, TILA)
│       └── datasets/
│           └── underwriting_rules.json
├── .gitignore
├── README.md
└── PROJECT_STATUS.md
```
