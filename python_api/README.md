# Panther Python API

A simplified Python implementation of the Panther phylogenetic tree API, based on the Java PruningController architecture.

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Edit `config.env` to customize settings:

```bash
# Basic configuration
FLASK_ENV=development
DEBUG=True
PORT=8080

# Mock data (set to false for real AWS S3 integration)
USE_MOCK_DATA=true

# Logging
LOG_LEVEL=INFO
LOG_FILE=panther_api.log

# CORS origins
ALLOWED_ORIGINS=http://localhost:8081,http://localhost:3000
```

### 3. Run the API

```bash
python panther_api.py
```

The API will be available at `http://localhost:8080`

## 🐳 Docker Deployment

### Quick Start with Docker Compose

1. **Copy environment template:**

   ```bash
   cp .env.example .env
   ```

2. **Run in development mode:**

   ```bash
   docker-compose --profile dev up --build
   ```

3. **Run in production mode:**
   ```bash
   docker-compose --profile prod up --build -d
   ```

The API will be available at `http://localhost:8080`

### Available Docker Commands

```bash
# Development (with hot reload)
docker-compose --profile dev up --build

# Production (with nginx reverse proxy)
docker-compose --profile prod up --build -d

# Stop all containers
docker-compose down

# View logs
docker-compose --profile dev logs -f

# Rebuild and restart
docker-compose --profile prod up --build -d --force-recreate
```

### Environment Configuration

Edit `.env` file for deployment:

- **Required**: Configure AWS credentials for S3 data access
- Update `ALLOWED_ORIGINS` with your frontend URLs
- The API only uses real S3 data (no mock data mode)

## 🧪 Testing

Import the Postman collection (`Panther_API_Postman_Collection.json`) for comprehensive API testing.
