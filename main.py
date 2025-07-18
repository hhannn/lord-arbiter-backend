# app/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dashboard import router as dashboard_router, lifespan

app = FastAPI(lifespan=lifespan)

# ✅ CORS setup — Add your deployed frontend if known
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://lord-arbiter.vercel.app",  # ← replace this if needed
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Mount your API routes
app.include_router(dashboard_router)

# ✅ Health check or root route
@app.get("/")
def read_root():
    return {"status": "OK"}

# ✅ Uvicorn dev mode (only used locally)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
