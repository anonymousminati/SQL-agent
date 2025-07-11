# SQL Database Agent

## 🚀 Overview

The **SQL Database Agent** is an intelligent assistant that translates natural language requests into accurate SQL queries. It streamlines complex database operations, ensuring data integrity and security.

## ✨ Features

- **Natural Language to SQL Translation:** Effortlessly converts plain English queries into precise SQL statements.
- **Schema Validation:** Checks database schema before executing queries to prevent errors.
- **Advanced SQL Support:** Handles complex joins and aggregations.
- **Query Optimization Suggestions:** Recommends ways to improve query performance.
- **Standardized Response Format:** Returns results in a consistent, easy-to-parse structure.
- **Comprehensive Logging:** Maintains detailed logs for audits and troubleshooting.

## 🛠️ Installation

1. **Clone the Repository**
   - Download project files from the repository.

2. **Configure Vertex AI Google Cloud API**
   - Follow the [Vertex AI Agent Development Kit Quickstart](https://cloud.google.com/vertex-ai/generative-ai/docs/agent-development-kit/quickstart).

3. **Create and Activate a Virtual Environment**
   ```shell
   python -m venv .venv
   ```
   - **Activate:**
     - macOS/Linux: `source .venv/bin/activate`
     - Windows CMD: `.venv\Scripts\activate.bat`
     - Windows PowerShell: `.venv\Scripts\Activate.ps1`

4. **Install Required Packages**
   ```shell
   pip install mysql-connector-python
   pip install google-adk
   ```

## 💡 Usage

1. **Open Terminal & Set Up Environment**
   ```powershell
   python -m venv venv
   venv\Scripts\activate
   pip install mysql-connector-python
   adk run SQL_Agent/
   ```
2. **Interact with the Agent**
   - Ask your database questions in natural language.

## ⚙️ Configuration

Edit the `DB_CONFIG` section in `toolList.py` to match your MySQL database settings:

```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password',
    'database': 'databasename',
    'port': 3306
}
```

## 🎬 Demo

A video demonstration is available below to showcase the agent's capabilities.
<!-- Video demo section will be shown here --> <div align="center"> <video width="600" controls> <source src="YOUR_VIDEO_LINK_HERE" type="video/mp4"> Your browser does not support the video tag. </video> </div>
  


## 🤝 Contributing

- Pull requests are welcome.
- For significant changes, please open an issue first to discuss your proposed modifications.

**Enjoy using the SQL Database Agent!**