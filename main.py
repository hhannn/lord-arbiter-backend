# app/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dashboard import router as dashboard_router

app = FastAPI()

# Allow frontend access (adjust origins in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:8000",
        "https://your-frontend.vercel.app",
    ],  # Replace with your frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the dashboard routes
app.include_router(dashboard_router)
