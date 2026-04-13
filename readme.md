# Wealth Asset Management Platform
### AI-Powered Financial Intelligence for Wealth Managers

---

## The business problem

Wealth management firms handle thousands of clients, each with unique portfolios spanning stocks, bonds, real estate, commodities, and cash. Wealth managers face three critical challenges:

1. **Data is locked behind SQL** — Extracting portfolio insights requires writing complex database queries. Most wealth managers and clients cannot write SQL, creating a bottleneck where every data request goes through a technical team.

2. **No real-time conversational insights** — Clients want instant answers about their portfolio health, risk exposure, and investment options. Traditional dashboards are static and cannot handle follow-up questions or personalized advice.

3. **Disconnected tools** — Portfolio data, risk analysis, and investment planning live in separate systems, making it impossible to get a unified view in one conversation.

## Our solution

We built an **AI-powered platform** that eliminates all three problems by combining:

- **A Text-to-SQL Copilot** — Ask questions in plain English, get instant answers from your database with auto-generated charts. No SQL knowledge needed.
- **An AI Financial Advisor Chatbot** — A conversational agent that fetches real client data, assesses portfolio risk, and creates personalized investment plans.
- **A unified data layer** — Both tools connect to the same Microsoft Fabric Data Warehouse, ensuring consistent, real-time data across every interaction.

### Before vs. After

| Before | After |
|--------|-------|
| "Email the data team and wait 2 days for a report" | "Type a question, get the answer in 3 seconds" |
| "Open 4 different tools to assess a client" | "One chat conversation covers everything" |
| "Only analysts can query the database" | "Anyone can ask questions in plain English" |
| "Static dashboards that cannot answer follow-ups" | "Dynamic AI that remembers context and follows up" |

---

## Demo

### SQL Copilot — Ask questions, get data + charts
![SQL Copilot Demo](screenshots/sql-copilot-demo.png)

**Example:** "Show me the top 5 clients with highest portfolio value"
- Auto-generates SQL query
- Returns real data in a formatted table
- Creates visualizations automatically
- Supports CSV download

### AI Financial Advisor — Conversational intelligence
![AI Chatbot Demo](screenshots/chatbot-demo.png)

**Example conversation:**
1. "I am client 42, show me my portfolio" → Fetches real portfolio data
2. "What are the risks?" → Analyzes risk and gives a score
3. "Suggest a plan to reduce risk" → Provides actionable investment advice

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Web Layer                            │
│  ┌──────────────────┐    ┌────────────────────────────┐  │
│  │  Vanna Flask UI  │    │  Flask Chatbot + Frontend  │  │
│  │   (port 8084)    │    │      (port 7001)           │  │
│  └────────┬─────────┘    └─────────────┬──────────────┘  │
├───────────┼────────────────────────────┼─────────────────┤
│           │         Agent Layer        │                  │
│           │  ┌──────────┐ ┌─────────┐ │                  │
│           │  │  Intent  │ │ Memory  │ │                  │
│           │  │Detection │ │(Window) │ │                  │
│           │  └──────────┘ └─────────┘ │                  │
│           │  ┌──────────────────────┐ │                  │
│           │  │   8 Financial Tools  │ │                  │
│           │  └──────────┬───────────┘ │                  │
├───────────┼─────────────┼─────────────┼─────────────────┤
│           │    Intelligence Layer      │                  │
│     ┌─────┴──────┐  ┌────────┐  ┌─────┴──────┐          │
│     │ Vanna.ai   │  │ChromaDB│  │ OpenAI GPT │          │
│     │ (Text2SQL) │  │(Vector)│  │(3.5 / 4o)  │          │
│     └─────┬──────┘  └────────┘  └────────────┘          │
├───────────┼─────────────────────────────────────────────┤
│           │          Data Layer                           │
│     ┌─────┴──────────────────────────────────────┐       │
│     │       Microsoft Fabric Data Warehouse       │       │
│     │  8 Tables │ 4 Views │ 15,100 rows │ pyodbc │       │
│     └────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────┘
```

## Technology stack

| Technology | Purpose |
|------------|---------|
| Python 3.12 | Core programming language |
| Microsoft Fabric | Cloud data warehouse |
| Vanna.ai | Text-to-SQL with RAG |
| LangChain | AI agent framework |
| OpenAI GPT-3.5 / GPT-4o-mini | LLM backbone |
| ChromaDB | Vector store for embeddings |
| Flask | Web server |
| pyodbc + ODBC Driver 18 | Database connectivity |
| Faker | Synthetic data generation |

---

## Project structure

```
WealthManagement/
├── Agent/                          # AI financial advisor chatbot
│   ├── agent.py                    # LangChain agent setup
│   ├── intent.py                   # Risk vs. investment intent detection
│   ├── memory.py                   # Smart conversation memory (window + truncation)
│   └── tools.py                    # 8 financial tools + database query tool
├── FinancialGoals/                 # Text-to-SQL engine
│   └── RAGToSQL/
│       ├── Helper/
│       │   ├── Credentials.py      # API keys, tokens, endpoints
│       │   ├── FabricsConnection.py # Database connection manager
│       │   └── VannaObject.py      # Vanna model configuration
│       ├── TrainingRAG-Artifact/   # Training data (schema, docs)
│       ├── FabricsRAG.py           # Core query engine with smart routing
│       ├── TrainRAG.py             # Model training script
│       ├── InferenceRAG.py         # CLI inference testing
│       ├── VisualizeRAG.py         # SQL Copilot web UI
│       └── train_extra.py         # Additional training examples
├── CreateDataWarehouse/            # Database setup
│   ├── SQL/                        # Table, view, procedure definitions
│   └── InsertToSQL.py              # Data population script (15,100 rows)
├── templates/
│   └── index.html                  # Chatbot frontend
├── app.py                          # Main chatbot server (port 7001)
├── connection.py                   # Azure token generator
└── requirements.txt
```

---

## Setup instructions

### Prerequisites
- Python 3.9+
- Microsoft Fabric workspace with Data Warehouse
- OpenAI API key
- ODBC Driver 18 for SQL Server

### Step 1: Clone and install
```bash
git clone https://github.com/YOUR_USERNAME/wealth-asset-management.git
cd wealth-asset-management
python -m venv myenv
myenv\Scripts\activate        # Windows
source myenv/bin/activate     # Mac/Linux
pip install -r requirements.txt
```

### Step 2: Configure credentials
Edit `FinancialGoals/RAGToSQL/Helper/Credentials.py`:
```python
class Credentials:
    sql_endpoint = "your-fabric-endpoint.datawarehouse.fabric.microsoft.com"
    database = "your_database_name"
    resource_url = "https://database.windows.net/.default"
    token = 'your-azure-token'
    open_ai_key = "your-openai-key"
    model = 'gpt-3.5-turbo-16k'
```

Also update the OpenAI key in `app.py`.

### Step 3: Get Azure token
```bash
python connection.py
# Follow the device code login, copy the token to Credentials.py
```

### Step 4: Set up the database
Run the SQL scripts from `CreateDataWarehouse/SQL/` in your Fabric Data Warehouse, then populate with data:
```bash
cd CreateDataWarehouse
python InsertToSQL.py
```

### Step 5: Train the AI model
```bash
cd FinancialGoals/RAGToSQL
python TrainRAG.py
python train_extra.py
```

### Step 6: Run the applications
**SQL Copilot:**
```bash
cd FinancialGoals/RAGToSQL
python VisualizeRAG.py
# Open http://localhost:8084
```

**AI Chatbot:**
```bash
python app.py
# Open http://localhost:7001
```

---

## Database schema

### Tables (15,100 total rows)
| Table | Description | Rows |
|-------|-------------|------|
| Advisors | Financial advisor profiles | 100 |
| Clients | Client profiles with risk ratings | 1,000 |
| Accounts | Savings, checking, investment accounts | 2,000 |
| Assets | Stocks, bonds, real estate, commodities, cash | 1,000 |
| Portfolios | Investment portfolios with risk levels | 1,000 |
| PortfolioAssets | Asset allocation within portfolios | 3,000 |
| Transactions | Buy/sell/deposit/withdraw history | 5,000 |
| Projections | Future value predictions | 2,000 |

### Views
| View | Purpose |
|------|---------|
| ClientPortfolioValue | Total portfolio value per client |
| PortfolioAssetAllocation | Asset distribution within portfolios |
| PortfolioSummary | Portfolio overview with asset counts |
| OverallWealthSummary | Wealth breakdown by asset type |

---

## Key features

- **Natural language to SQL** — Ask questions in English, get database results instantly
- **Auto-generated visualizations** — Charts and tables created automatically from query results
- **Conversational AI advisor** — Multi-turn conversations with context memory
- **Real-time data** — Both interfaces query the live database, no stale data
- **Self-improving model** — Train new question-SQL pairs to expand capabilities
- **Auto-fix SQL** — AI can analyze and correct its own SQL errors
- **Per-user memory** — Chatbot remembers conversation context for personalized responses
- **Intent detection** — Automatically routes between risk analysis and investment planning

---

## Sample queries

### SQL Copilot
```
Show me the top 5 clients with highest portfolio value
How many transactions happened in 2023?
For each asset type show the count minimum maximum and average value
Count the number of portfolios for each risk level
Show clients with portfolio value above 3000 with their advisor name
```

### AI Chatbot
```
I am client 42, show me my portfolio
What are the risks in my portfolio?
Suggest a risk mitigation plan
I want to save $500,000 for a house in 5 years
Show me an alternative investment plan
```

---

## Lessons learned

- Microsoft Fabric Data Warehouse uses different SQL syntax than standard SQL Server (no PRIMARY KEY, FOREIGN KEY, NVARCHAR, or DATETIME)
- Azure authentication requires DeviceCodeCredential for Fabric-only accounts
- AI models need targeted training examples for complex multi-table queries
- Conversation memory must be truncated to prevent agent context overflow
- Direct SQL queries outperform AI-generated SQL for common patterns (client lookups)

---

## Future improvements

- [ ] Deploy to cloud for 24/7 availability
- [ ] Add user authentication and role-based access
- [ ] Integrate real market data feeds
- [ ] Build a Power BI dashboard layer
- [ ] Add more training data for increasingly complex queries
- [ ] Implement streaming responses for faster chat experience

---

## License

This project is for educational and demonstration purposes.
