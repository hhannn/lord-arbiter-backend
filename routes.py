from fastapi import APIRouter
from app.models import StartBotRequest, BotStatus
from app.services.bot import start_bot, stop_bot, get_status

router = APIRouter()

@router.post("/start")
async def start(request: StartBotRequest):
    return await start_bot(request)

@router.post("/stop")
async def stop():
    return stop_bot()

@router.get("/status", response_model=BotStatus)
async def status():
    return get_status()
