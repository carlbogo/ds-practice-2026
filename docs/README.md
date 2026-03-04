# Distributed Systems @ University of Tartu

This repository contains the initial code for the practice sessions of the Distributed Systems course at the University of Tartu.

## Getting started

### Overview

The code consists of multiple services. Each service is located in a separate folder. The `frontend` service folder contains a Dockerfile and the code for an example bookstore application. Each backend service folder (e.g. `orchestrator` or `fraud_detection`) contains a Dockerfile, a requirements.txt file and the source code of the service. During the practice sessions, you will implement the missing functionality in these backend services, or extend the backend with new services.

There is also a `utils` folder that contains some helper code or specifications that are used by multiple services. Check the `utils` folder for more information.
### System diagram
```mermaid
sequenceDiagram
    participant User
    participant Frontend
    participant Orchestrator
    participant Verification
    participant Fraud
    participant Suggestions

    User->>Frontend: Submit order
    Frontend->>Orchestrator: POST /checkout

    par Parallel gRPC Calls
        Orchestrator->>Verification: VerifyTransaction
        Orchestrator->>Fraud: DetectFraud
        Orchestrator->>Suggestions: GetSuggestions
    end

    Verification-->>Orchestrator: is_valid / reasons
    Fraud-->>Orchestrator: is_fraud / reasons
    Suggestions-->>Orchestrator: suggested_books

    Orchestrator->>Orchestrator: Decision logic

    Orchestrator-->>Frontend: Approved or Rejected
    Frontend-->>User: Display result
```

### Setting up the Groq API Key

The `suggestions` service uses Groq AI to generate book recommendations. To use this service, you need to obtain a Groq API key:

1. **Get a Groq API Key:**
   - Go to [https://console.groq.com/](https://console.groq.com/)
   - Sign up for a free account
   - Navigate to API Keys section
   - Create a new API key and copy it

2. **Create a `.env` file:**
   - In the root directory of the repository, create a file named `.env`
   - Add your Groq API key to the file:
   ```
   GROQ_API_KEY=your_api_key_here
   ```
   - Replace `your_api_key_here` with your actual API key

3. **The `.env` file is already configured in `docker-compose.yaml`** and will be automatically loaded by the suggestions service.

**Note:** The `.env` file should never be committed to version control (it's already in `.gitignore`). Each team member should create their own `.env` file with their own API key.

### Running the code with Docker Compose [recommended]

To run the code, you need to clone this repository, make sure you have Docker and Docker Compose installed, and run the following command in the root folder of the repository:

```bash
docker compose up
```

This will start the system with the multiple services. Each service will be restarted automatically when you make changes to the code, so you don't have to restart the system manually while developing. If you want to know how the services are started and configured, check the `docker-compose.yaml` file.

The checkpoint evaluations will be done using the code that is started with Docker Compose, so make sure that your code works with Docker Compose.

If, for some reason, changes to the code are not reflected, try to force rebuilding the Docker images with the following command:

```bash
docker compose up --build
```

### Run the code locally

Even though you can run the code locally, it is recommended to use Docker and Docker Compose to run the code. This way you don't have to install any dependencies locally and you can easily run the code on any platform.

If you want to run the code locally, you need to install the following dependencies:

backend services:
- Python 3.8 or newer
- pip
- [grpcio-tools](https://grpc.io/docs/languages/python/quickstart/)
- requirements.txt dependencies from each service

frontend service:
- It's a simple static HTML page, you can open `frontend/src/index.html` in your browser.

And then run each service individually.
