# checkout-service

Main checkout orchestration service

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally  
python src/main.py

# Server starts on http://localhost:8000
```

## 🐳 Docker

```bash
# Build image
docker build -t checkout-service:v1 .

# Run container
docker run -p 8000:8000 checkout-service:v1
```

## ☸️ Kubernetes Deployment

```bash
# Deploy with Helm
helm install checkout-service ./helm \
  --namespace ecommerce \
  --create-namespace

# Port forward to test
kubectl port-forward svc/checkout-service 8000:8000 -n ecommerce
```

## 📁 Project Structure

```
checkout-service/
├── src/
│   ├── main.py          # Application entry point
│   ├── models/          # Data models
│   ├── routes/          # API routes
│   └── utils/           # Utilities
├── tests/               # Test files
├── helm/                # Helm chart
├── Dockerfile           # Container definition
└── requirements.txt     # Python dependencies
```

## 🔧 Configuration

Environment variables:
- `DATABASE_URL` - Database connection string (if applicable)
- `PORT` - Service port (default: 8000)
- Other service-specific configs

## 🛠️ Tech Stack

- Python 3.11
- FastAPI
- SQLAlchemy (if database)
- PostgreSQL (if database)
- Pydantic

## 📊 API Documentation

Once running, visit: http://localhost:8000/docs
# checkout-service
