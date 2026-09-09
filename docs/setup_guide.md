# Setup & Running Guide

This guide explains how to start the Distributed Key-Value Store backend and the React Dashboard frontend.

## Prerequisites
- **Go 1.21+** (for the backend server)
- **Node.js 18+** & **npm** (for the frontend dashboard)

## Directory Structure
The repository is split into two main directories:
- `kv-store/`: The Go backend containing the Raft implementation, WAL, Store, and HTTP API.
- `frontend/`: The React + Vite frontend dashboard.

---

## 1. Starting the Backend

The backend spins up a 3-node Raft cluster in a single process for local testing. It binds to port `8080`.

1. Open a terminal.
2. Navigate to the backend directory:
   ```bash
   cd kv-store
   ```
3. Run the server:
   ```bash
   go run ./cmd/server
   ```

*To gracefully shut down the cluster and flush all data to the WAL, press `Ctrl + C`.*

---

## 2. Starting the Frontend Dashboard

The frontend is a Vite + React application. It uses a proxy to route API requests to `localhost:8080` to avoid CORS issues during development.

1. Open a **new** terminal tab.
2. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```
3. Install dependencies (only required the first time):
   ```bash
   npm install
   ```
4. Start the development server:
   ```bash
   npm run dev
   ```
5. Open your browser and navigate to: `http://localhost:5173/`

*To stop the frontend, press `Ctrl + C` in this terminal.*

---

## 3. Cleaning Up Data (Optional)

The backend persists data using Write-Ahead Logs (WAL) in the `kv-store/data/` directory. If you want to completely wipe the cluster state and start fresh:

1. Ensure the backend server is stopped.
2. Delete the `data` directory:
   - Linux/Mac: `rm -rf kv-store/data/`
   - Windows (PowerShell): `Remove-Item -Recurse -Force kv-store/data`
3. Restart the backend server. It will boot from a clean state.
