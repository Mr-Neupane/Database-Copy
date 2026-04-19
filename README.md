# Database-Copy

A lightweight console application for **database migration and version synchronization** between **Microsoft SQL Server (MSSQL)** and **PostgreSQL (PSQL)**.

---

## 🚀 Features

### 🔄 Cross-Database Migration

* **MSSQL → PostgreSQL**

  * Transfers schema (tables, constraints, indexes)
  * Converts MSSQL data types to PostgreSQL equivalents
  * Migrates table data

* **PostgreSQL → MSSQL**

  * Reverse migration support
  * Handles PostgreSQL-specific structures
  * Converts types into MSSQL-compatible formats

---

### 🔁 MSSQL Version Downgrade / Sync

* Sync a **higher version MSSQL database** to a **lower version**
* Automatically:

  * Compares schema differences
  * Generates required scripts
  * Applies structural adjustments
* Useful for:

  * Environment consistency (dev → staging → production)
  * Rollbacks
  * Legacy system compatibility

---

## 🏗️ How It Works

1. Connect to **source database**
2. Read schema & metadata
3. Map data types between engines
4. Generate compatible schema
5. Transfer data safely
6. Apply constraints and indexes

---

## ⚙️ Configuration

Update your connection settings before running:

```json
﻿{
  "ConnectionStrings": {
    "MSSQL": "Server=localhost;Database=MyAppDb;User Id=sa;Password=pass;TrustServerCertificate=True;",
    "PostgresSQL": "Host=localhost;Port=5432;Database=test;Username=postgres;Password=pass;"
  }
}
```

## 🔧 Data Type Mapping (Examples)

| MSSQL    | PostgreSQL |
| -------- | ---------- |
| INT      | INTEGER    |
| NVARCHAR | TEXT       |
| DATETIME | TIMESTAMP  |
| BIT      | BOOLEAN    |

---
## 🤝 Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Open a pull request

---

## 👨‍💻 Author

Maintained by the repository owner:
👉 https://github.com/Mr-Neupane

---
