import sys
import logging
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import asyncpg

from app.config.settings import settings
from app.database.connection import db_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("liberstack")

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="liberstack - monitor público de consulta e assistência médica",
    description="API resiliente orientada ao impacto social e transparência de dados públicos de saúde.",
    version="1.0.0"
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Isolamento e Zero Trust de rede na borda da aplicação
ALLOWED_ORIGINS = ["https://liberstack.com.br"] if settings.ENV == "production" else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"], 
    allow_headers=["Authorization", "Content-Type"],
)

@app.on_event("startup")
async def startup_event():
    logger.info("iniciando ciclo de boot da aplicação...")
    try:
        # inicialização do pool de conexões
        await db_manager.connect()
        
        # fail fast para evitar subir contêineres "zumbi"
        await db_manager.execute_safely("SELECT 1")
        logger.info("verificação de sanidade do banco concluída com sucesso. sistema pronto para tráfego.")
    except Exception as e:
        logger.critical(f"falha crítica na inicialização: banco de dados inacessível: {e}")
        sys.exit(1)

@app.on_event("shutdown")
async def shutdown_event():
    await db_manager.disconnect()

@app.middleware("http")
async def validation_exception_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except asyncpg.TooManyConnectionsError:
        logger.error(f"alerta de infraestrutura: pool esgotado para o IP {get_remote_address(request)}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "erro": "serviço temporariamente sobrecarregado",
                "mensagem": "estamos garantindo que as consultas sejam entregues com segurança. tente novamente em alguns instantes."
            }
        )

# healthcheck: rota de monitoramento exposta 
@app.get("/v1/saude", tags=["Monitoramento"])
@limiter.limit("10/minute")
async def health_check(request: Request):
    db_stats = db_manager.get_pool_stats()
    
   # se o pool estiver sem conexões, avisa o orquestrador 
    status_code = status.HTTP_200_OK
    if db_stats.get("status") == "congested":
        status_code = status.HTTP_207_MULTI_STATUS
        
    return JSONResponse(
        status_code=status_code,
        content={
            "api": "online",
            "ambiente": settings.ENV,
            "infraestrutura": {
                "banco_dados": db_stats
            }
        }
    )