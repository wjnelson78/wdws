# WDWS Agent System

A comprehensive legal workflow automation system featuring email management, case tracking, and Model Context Protocol (MCP) servers for intelligent document processing and legal analysis.

## Features

### Email Management
- Automated email ingestion and classification
- intelligent email triage using GPT-4o
- Attachment extraction and processing
- Email-to-case linking and tracking

### Case Management
- Legal case tracking and organization
- Timeline generation from case documents
- Strategic case analysis
- Document relationship mapping

### MCP Servers
- Legal cases MCP server
- Medical records MCP server
- Paperless-NGX integration
- OAuth authentication support

### Intelligent Agents
- **Email Triage Agent**: Automatically categorizes and routes emails
- **Case Strategy Agent**: Provides strategic legal analysis
- **Timeline Agent**: Generates chronological case timelines
- **Data Quality Agent**: Ensures data integrity and completeness
- **Query Insight Agent**: Provides semantic search and analysis
- **Security Agent**: Monitors and protects sensitive information
- **Self-Healing Agent**: Automatically detects and fixes issues
- **Watchdog Agent**: Monitors system health and performance

### Dashboard
- Real-time case and email monitoring
- Interactive chat interface with AI agents
- Document search and retrieval
- System status and health monitoring

## Architecture

```
/opt/wdws/
├── agents/              # AI agent framework and implementations
├── dashboard/           # Web-based dashboard interface
├── data/               # Data storage (cases, emails, medical records)
├── mcp-server/         # Main MCP server implementation
├── mcp-servers/        # Specialized MCP servers
│   └── paperless-ngx/  # Paperless-NGX integration
├── docx-proxy/         # Document proxy service
├── imessage-proxy/     # iMessage integration proxy
├── migrations/         # Database schema migrations
└── patches/            # System patches and utilities
```

## Prerequisites

- Python 3.8+
- PostgreSQL database
- OpenAI API key
- nginx (for proxies)

## Environment Variables

Create a `.env` file in the project root with the following variables:

```bash
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/wdws

# OpenAI
OPENAI_API_KEY=your_openai_api_key_here

# Optional
OPENAI_MODEL=gpt-4o
EMBEDDING_MODEL=text-embedding-3-large
EMAILS_DIR=/opt/wdws/data/emails
DATA_DIR=/opt/wdws/data
```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/wjnelson78/wdws.git
   cd wdws
   ```

2. Install Python dependencies:
   ```bash
   pip install -r agents/requirements.txt
   ```

3. Set up the database:
   ```bash
   psql -U postgres -f migrations/001_enterprise_schema.sql
   psql -U postgres -f 002_email_management.sql
   psql -U postgres -f 003_attachment_support.sql
   psql -U postgres -f agents/migrations/005_agent_system.sql
   ```

4. Configure environment variables (see above)

## Usage

### Start the Agent System
```bash
cd agents
python run.py
```

Or use the systemd service:
```bash
sudo systemctl start wdws-agents
```

### Start the Dashboard
```bash
cd dashboard
python app.py
```

### Email Synchronization
```bash
python email_sync.py
```

### Attachment Ingestion
```bash
./run_attach.sh
```

### Document Ingestion
```bash
./start_ingest.sh
```

## MCP Server

The MCP server provides a Model Context Protocol interface for AI assistants to interact with the legal workflow system.

Start the MCP server:
```bash
cd mcp-server
python mcp_server.py
```

## Security

⚠️ **Important Security Notes:**
- Never commit `.env` files or API keys to version control
- All sensitive credentials should be stored in environment variables
- The system handles sensitive legal and medical information - ensure proper access controls
- Use HTTPS in production environments
- Regularly update dependencies for security patches

## License

Proprietary - All rights reserved

## Support

For issues and questions, please contact the development team.
