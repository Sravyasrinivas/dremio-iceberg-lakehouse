# Building a Data Lakehouse on Your Laptop — Dremio + Spark + Nessie + MinIO + Apache Iceberg

A complete, from-scratch project that runs a modern **data lakehouse** entirely on
your own machine using free, open-source tools inside Docker. You will store data
in object storage, organize it as **Apache Iceberg** tables tracked by a
**Nessie** catalog, and query and manipulate it with two engines — **Dremio**
(SQL) and **Apache Spark** (Python). The demo data is a small, synthetic
card-transactions dataset.

If you have never heard of any of these words, that is fine — this README explains
every concept from zero, tells you exactly what to install, and walks you through
the whole thing step by step. It is written so that someone with no prior
background can reproduce the project end to end.

---

## Table of contents

1. [The big picture (plain English)](#1-the-big-picture-plain-english)
2. [Concepts explained from zero](#2-concepts-explained-from-zero)
3. [Glossary of every term used](#3-glossary-of-every-term-used)
4. [Software you need to install](#4-software-you-need-to-install)
5. [The services in this project](#5-the-services-in-this-project)
6. [Step-by-step walkthrough](#6-step-by-step-walkthrough)
7. [The Spark notebooks: time travel & branching](#7-the-spark-notebooks-time-travel--branching)
8. [Version notes (important)](#8-version-notes-important)
9. [Troubleshooting](#9-troubleshooting)
10. [Shutting it down](#10-shutting-it-down)
11. [References & further reading](#11-references--further-reading)
12. [Credits](#12-credits)

---

## 1. The big picture (plain English)

Companies collect huge amounts of data. They need somewhere to **store** it,
somewhere to **organize** it into tables, and a way to **query** it (ask
questions like "how much did customers spend last month?").

Historically there were two approaches:

- A **data warehouse** — neat, structured, fast for SQL, but expensive and rigid.
- A **data lake** — cheap storage that holds any raw files, but messy and hard to
  query reliably.

A **data lakehouse** combines both: the cheap storage of a lake **plus** the
reliable tables, transactions, and SQL of a warehouse. This project builds a tiny
but real lakehouse:

```
  You generate CSV/Parquet data
              │
              ▼
   ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
   │    MinIO     │      │    Nessie    │◄────►│    Dremio    │  ← ask SQL questions
   │  (storage)   │◄────►│  (catalog)   │      │  (SQL engine)│
   │  the "lake"  │      │ tracks tables│      └──────────────┘
   └──────────────┘      │ + versions   │      ┌──────────────┐
      raw + Iceberg      └──────────────┘◄────►│    Spark     │  ← transform data
        files                                  │ (Python)     │     with code
                                               └──────────────┘
```

- **MinIO** is the storage (the "lake").
- **Apache Iceberg** is the format that turns raw files into proper tables.
- **Nessie** is the catalog that keeps track of those tables — and, like Git,
  can create branches and roll back changes.
- **Dremio** and **Spark** are the two engines that read and write the data.
- **Docker** runs all of these on your laptop without manual installation.

---

## 2. Concepts explained from zero

### Object storage / MinIO
**Object storage** is a way to store files ("objects") in flat "buckets" instead
of folders on a normal disk. Amazon **S3** is the most famous example. It is cheap
and scales to enormous sizes, which is why lakehouses use it.
**MinIO** is free software that behaves exactly like Amazon S3 but runs on your
own machine — so you can learn S3 concepts locally for free. In this project MinIO
has two buckets: `landing` (raw incoming files) and `warehouse` (the Iceberg
tables).

### Table format / Apache Iceberg
Object storage only holds files. To treat a pile of files as a real **table**
(with columns, row inserts/updates/deletes, and history), you need a **table
format**. **Apache Iceberg** is the leading open table format. It adds:
- **ACID transactions** — safe concurrent reads/writes, no half-written data.
- **Schema evolution** — add or rename columns without rewriting everything.
- **Snapshots & time travel** — every change creates a snapshot you can query
  later ("show me the table as it was yesterday").
- **Rollback** — restore the table to an earlier snapshot.

Iceberg's main competitor is **Delta Lake** (used by Databricks). They solve the
same problem.

### Catalog / Nessie
A **catalog** is the "phone book" that maps table names to their files and current
state. Engines ask the catalog "where is table X and what's its latest version?"
**Nessie** is a catalog with a superpower: **Git-like branching**. You can:
- create a **branch** of the whole catalog,
- make risky changes on the branch in isolation,
- **merge** them into `main` if good, or **drop** the branch to discard them —
all without affecting production data. This is often called "data as code."
The other popular modern catalog is **Polaris** (also open source).

### Query engine / Dremio
A **query engine** is the software that actually runs your SQL and returns
answers. **Dremio** is a free, fast lakehouse query engine with a friendly web UI.
It reads raw files from MinIO, writes curated Iceberg tables into Nessie, and lets
you run SQL and even query specific Nessie branches.

### Apache Spark / PySpark
**Apache Spark** is a widely used engine for processing large data with code
(rather than only SQL). **PySpark** is Spark's Python interface. In this project
Spark is a *second* engine that writes Iceberg tables and drives Nessie branching
from **Jupyter notebooks** (interactive Python documents). Using two engines on
one catalog proves the lakehouse is **engine-agnostic**.

### Docker, images, containers, Compose
Installing Dremio, MinIO, Nessie and Spark by hand is painful. **Docker** solves
this. Key ideas:
- A **container** is a lightweight, isolated mini-environment that runs one piece
  of software with everything it needs bundled in.
- An **image** is the downloadable blueprint a container is created from.
- **Docker Compose** is a tool that starts *several* containers together from one
  file (`docker-compose.yml`). Running `docker compose up` launches the entire
  stack at once.

### Bronze / silver (the "medallion" pattern)
A common way to organize a lakehouse in layers:
- **Bronze** = raw, as-ingested data (here: the parquet files in `landing`).
- **Silver** = cleaned, curated tables (here: the Iceberg tables in `warehouse`).
- (Gold = business-level aggregates — not built here, but the idea extends.)

---

## 3. Glossary of every term used

| Term | Meaning |
|---|---|
| **Data lake** | Cheap storage holding raw files of any kind. |
| **Data warehouse** | Structured, SQL-optimized store; reliable but pricier/rigid. |
| **Data lakehouse** | Combines lake storage with warehouse-style tables & SQL. |
| **Object storage** | Storing files as "objects" in "buckets" (e.g. Amazon S3). |
| **S3** | Amazon's object storage service and its widely-copied API. |
| **MinIO** | Free, self-hosted, S3-compatible object storage. |
| **Bucket** | A top-level container for objects in object storage. |
| **Parquet** | A compact, columnar file format for tabular data. |
| **Table format** | Rules that make a set of files behave as one table (Iceberg). |
| **Apache Iceberg** | Open table format with ACID, snapshots, time travel. |
| **Delta Lake** | Iceberg's main alternative, associated with Databricks. |
| **ACID** | Atomic, Consistent, Isolated, Durable — safe transactions. |
| **Snapshot** | A saved version of a table at a point in time. |
| **Time travel** | Querying a table as it existed at an earlier snapshot. |
| **Rollback** | Restoring a table to an earlier snapshot. |
| **Catalog** | Directory mapping table names to their files & state. |
| **Nessie** | Catalog with Git-like branching/merge for data. |
| **Polaris** | Another modern open catalog (alternative to Nessie). |
| **Branch (data)** | An isolated line of catalog changes, like a Git branch. |
| **Commit (data)** | A recorded change to the catalog (Nessie is versioned). |
| **Query engine** | Software that executes queries (Dremio, Spark, Trino…). |
| **Dremio** | Free lakehouse SQL engine with a web UI. |
| **Apache Spark** | Engine for large-scale data processing with code. |
| **PySpark** | Spark's Python API. |
| **Jupyter notebook** | Interactive document mixing code, output, and notes. |
| **JVM** | Java Virtual Machine; Spark/Iceberg/Nessie run on it. |
| **JAR** | A packaged Java library file (`.jar`). |
| **Docker** | Tool that runs software in isolated containers. |
| **Image** | Blueprint used to create a container. |
| **Container** | A running, isolated instance of an image. |
| **Docker Compose** | Runs multiple containers together from one YAML file. |
| **Medallion (bronze/silver/gold)** | Layered lakehouse data organization. |
| **CTAS** | `CREATE TABLE AS SELECT` — build a table from a query. |

---

## 4. Software you need to install

You only need three things on your computer. Everything else runs inside Docker.

1. **Docker Desktop** — runs the whole stack.
   Download: https://www.docker.com/products/docker-desktop/
   After installing, open Docker Desktop once and let it finish starting. Give it
   at least ~8 GB RAM in Settings → Resources (the Spark image is large).

2. **Git** — to clone this repo and track your own changes.
   Download: https://git-scm.com/downloads
   (On Windows this also installs "Git Bash", a handy terminal.)

3. **Python 3.10+** — only used to generate the sample data.
   Download: https://www.python.org/downloads/
   On Windows, tick **"Add python.exe to PATH"** during install. If the `python`
   command is hijacked by the Microsoft Store, use `py` instead.

Verify each in a terminal:

```bash
docker --version
git --version
python --version   # or: py --version
```

---

## 5. The services in this project

`docker-compose.yml` defines four services:

| Service | Image | Purpose | Open at |
|---|---|---|---|
| **minio** | `minio/minio` | Object storage (the lake) | Console: http://localhost:9001 |
| **nessie** | `ghcr.io/projectnessie/nessie` | Catalog with branching | API: http://localhost:19120 |
| **dremio** | `dremio/dremio-oss` | SQL query engine + UI | http://localhost:9047 |
| **spark** | `alexmerced/spark35nb` | Spark + Jupyter notebooks | http://localhost:8888 |

Default demo credentials for MinIO: **`minioadmin` / `minioadmin`** (never reuse
these anywhere real).

---

## 6. Step-by-step walkthrough

### Step 0 — Get the project

```bash
git clone <your-fork-url> dremio-iceberg-lakehouse
cd dremio-iceberg-lakehouse
cp .env.example .env
```

### Step 1 — Start the core stack

```bash
docker compose up -d minio nessie dremio
docker compose ps          # all should show "Up" (minio-init exits after making buckets)
```

Check:
- MinIO console http://localhost:9001 (login `minioadmin`/`minioadmin`) shows two
  buckets: `landing` and `warehouse`.
- Dremio http://localhost:9047 loads (first boot can take 1–2 minutes). Create an
  admin account.

### Step 2 — Generate and upload sample data

```bash
python -m venv .venv
source .venv/bin/activate          # Windows Git Bash: source .venv/Scripts/activate
pip install -r requirements.txt
python data/generate_card_data.py --upload   # writes parquet, pushes to MinIO landing
```

This creates three synthetic tables — `accounts`, `cards`, `transactions` — as
Parquet files and uploads them to the MinIO `landing` bucket.

### Step 3 — Add data sources in Dremio

In the Dremio UI (http://localhost:9047) add two sources. **Full click-by-click
settings, including the tricky connection properties, are in
[`RUNBOOK.md`](RUNBOOK.md).** In short:

- **`minio_landing`** (type: S3) → points at MinIO, "Enable compatibility mode",
  endpoint `minio:9000`, path-style access `true`, SSL `false`.
- **`lakehouse`** (type: Nessie) → endpoint `http://nessie:19120/api/v2`, storage
  in the MinIO `warehouse` bucket with the same S3 properties.

> Use the container names `minio` and `nessie` (not `localhost`) in these
> settings — Dremio reaches the other services over Docker's internal network.

### Step 4 — Promote raw data to Iceberg, then analyze

In the Dremio SQL Runner, run the scripts in `sql/`:

- `sql/01_promote_to_iceberg.sql` — reads the raw parquet and creates Iceberg
  tables (`lakehouse.cards.accounts`, `.cards`, `.transactions`). This is the
  bronze → silver step.
- `sql/02_analytics.sql` — example analytics (spend by category, decline rates,
  top accounts, etc.).

### Step 5 — Start Spark and run the notebooks

See the next section.

---

## 7. The Spark notebooks: time travel & branching

Start the Spark container (first pull is a few GB — one-time):

```bash
docker compose up -d spark
docker logs spark          # copy the printed http://127.0.0.1:8888/lab?token=... URL
```

Open that URL, go into the `spark/` folder in the Jupyter file browser, and run
the two notebooks cell by cell (Shift+Enter):

- **`01_time_travel.ipynb`** — creates a small Iceberg table, makes a bad update
  and a delete (each a new **snapshot**), lists the snapshot history, queries the
  table `VERSION AS OF` an old snapshot (**time travel**), and then **rolls back**
  the live table to the clean state.

- **`02_nessie_branching.ipynb`** — creates a Nessie **branch** off `main`, makes
  a destructive change *only on the branch*, shows `main` is untouched, then either
  **merges** the branch or **drops** it to revert.

### Bonus: read a Spark branch from Dremio

Because both engines share the Nessie catalog, after Spark creates a branch you
can query it from Dremio's SQL Runner (don't drop the branch first):

```sql
SELECT 'main' AS ref, COUNT(*) FROM lakehouse.demo.card_txns AT BRANCH "main"
UNION ALL
SELECT 'etl_experiment', COUNT(*) FROM lakehouse.demo.card_txns AT BRANCH "etl_experiment";
```

Same table, two branches, different data, one engine reading what the other wrote.

---

## 8. Version notes (important)

The Spark image ships **Java 11**, so the Iceberg and Nessie Java libraries
(`.jar` files) must be Java-11 builds and version-matched. Spark, Iceberg, and
Nessie all run on the **JVM** — PySpark is just a Python front door to a Java
engine — so a library compiled for a newer Java will refuse to load. This exact
set is verified to work:

| Component | Version |
|---|---|
| Spark image | `alexmerced/spark35nb` (Spark 3.5, Java 11) |
| `iceberg-spark-runtime-3.5_2.12` | 1.5.0 |
| `nessie-spark-extensions-3.5_2.12` | 0.77.1 |
| `software.amazon.awssdk:bundle` + `url-connection-client` | 2.24.8 |
| Nessie API used by **Spark** | **v1** (`http://nessie:19120/api/v1`) |
| Nessie API used by **Dremio** | v2 (`http://nessie:19120/api/v2`) |

These packages are pulled automatically by the notebooks via
`spark.jars.packages` in the first cell.

---

## 9. Troubleshooting

| Symptom | Cause & fix |
|---|---|
| `python: Permission denied` (Windows) | The Microsoft Store stub. Use `py` instead of `python`. |
| `.venv/bin/activate: No such file` (Windows) | Use `source .venv/Scripts/activate`. |
| Dremio S3 source: "Could not connect" | Turn on "Enable compatibility mode"; set `fs.s3a.endpoint=minio:9000`, `fs.s3a.path.style.access=true`, `fs.s3a.connection.ssl.enabled=false`. |
| Dremio Nessie source: "Credential Verification failed" | On the Storage tab add `dremio.s3.compat=true` (and the three `fs.s3a.*` props). |
| Dremio: "Object … not found" for parquet | Promote the folder: hover the folder in the source → Format Folder → Save. |
| Spark: `ClassNotFoundException … NessieSparkSessionExtensions` | The extension JAR isn't loaded; ensure `spark.jars.packages` is set and **restart the kernel**. |
| Spark: `Cannot find catalog plugin … SparkCatalog` | The Iceberg runtime JAR isn't loaded; add it to `spark.jars.packages`. |
| Spark: `UnsupportedClassVersionError … class file version 61.0` | JAR built for Java 17 on a Java 11 image. Use the pinned Java-11 versions above. |
| Spark: `NessieApiCompatibilityException (expected 1, actual 2)` | Change the Spark catalog URI to `…/api/v1`. |
| Docker: "no space left on device" | Free space: `docker image prune -a`, or raise Docker Desktop's disk limit. |

---

## 10. Shutting it down

```bash
docker compose down        # stop containers, keep data volumes
docker compose down -v     # also delete all stored data (fresh start next time)
```

Note: Nessie here uses an in-memory store, so a full `down` resets the catalog.
The RUNBOOK shows how to make it persistent if you want data to survive restarts.

---

## 11. References & further reading

**Official documentation**
- Apache Iceberg — https://iceberg.apache.org/docs/latest/
- Project Nessie — https://projectnessie.org/
- Nessie + Iceberg + Spark guide — https://projectnessie.org/iceberg/spark/
- MinIO documentation — https://min.io/docs/minio/container/index.html
- Dremio documentation — https://docs.dremio.com/
- Apache Spark — https://spark.apache.org/docs/latest/
- Docker: Get Started — https://docs.docker.com/get-started/

**Concepts & tutorials**
- Dremio — Intro to Dremio, Nessie & Iceberg on your laptop:
  https://www.dremio.com/blog/intro-to-dremio-nessie-and-apache-iceberg-on-your-laptop/
- Dremio — Hands-on with Iceberg, Nessie, Dremio & Spark:
  https://www.dremio.com/blog/hands-on-with-apache-iceberg-nessie-dremio-apache-spark/
- MinIO — Data lake with Nessie, Dremio & Iceberg:
  https://www.min.io/blog/uncover-data-lake-nessie-dremio-iceberg
- Apache Iceberg catalogs explained (REST, Glue, Hive, Polaris, Nessie):
  https://iceberglakehouse.com/posts/2026-05-22-apache-iceberg-catalogs-explained/
- Nessie as an Iceberg REST catalog:
  https://www.dremio.com/blog/use-nessie-with-iceberg-rest-catalog/

**Reference / package coordinates**
- iceberg-spark-runtime on Maven Central:
  https://central.sonatype.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12/versions
- nessie-spark-extensions on Maven Central:
  https://central.sonatype.com/artifact/org.projectnessie.nessie-integrations/nessie-spark-extensions-3.5_2.12

---

## 12. Credits

The Docker environment and local lakehouse approach are based on tutorials by
**Alex Merced** (Dremio) and the Dremio Developer Advocacy team. This repo
modernizes and re-documents that setup as an end-to-end learning project.

Built with free, open-source software: Apache Iceberg, Project Nessie, MinIO,
Apache Spark, and Dremio Community Edition.
