## Panther API (Python)

Flask-based API exposing core Panther phylogenetic operations. This is the service API (not the data-processing scripts).

### What it provides

- **Grafting**: add a sequence to a Panther tree.
- **Pruning**: return a pruned tree for selected taxa.
- **FASTA export**: download sequences for a tree (full or pruned).
- **Ortholog mapping**: fetch orthologs for a gene and organism.

Key endpoints (see `Panther_API_Postman_Collection.json` for full usage):

- `POST /panther/grafting`
- `POST /panther/pruning/<treeId>`
- `POST /panther/grafting/prune`
- `POST /panther/fastadoc/<treeId>` (alias: `/panther/pruning/fastadoc/<treeId>`)
- `POST /panther/orthomapping`
- `GET /health`

---

### Install (local)

1. Python 3.11+ and pip
2. From this folder:

```bash
pip install -r requirements.txt
```

3. Create `.env` (minimal):

```bash
FLASK_ENV=development
DEBUG=true
PORT=8080
LOG_LEVEL=INFO
ALLOWED_ORIGINS=http://localhost:8081,http://localhost:3000
# Optional/when using S3-backed data
AWS_ACCESS_KEY=...
AWS_SECRET_KEY=...
AWS_REGION=us-west-2
PG_TREE_BUCKET=phg-panther-data-19
PG_MSA_BUCKET=phg-panther-msa-data-19
```

4. Run:

```bash
python panther_api.py
```

Open `http://localhost:8080/health`.

### Install with Docker

```bash
# Development (hot reload)
docker-compose --profile dev up --build

# Production
docker-compose --profile prod up --build -d
```

Logs (example):

```bash
docker logs -f panther-api-dev
```

---

### Notes

- CORS is controlled via `ALLOWED_ORIGINS`.
- Logs go to stdout; a file is also created in `python_api_scripts/logs/` when permitted.
- Import `Panther_API_Postman_Collection.json` for example requests.
