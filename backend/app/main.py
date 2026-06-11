import sys
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import asyncpg

from app.config.settings import settings
from app.database.connection import gerenciador_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("liberstack")

limiter = Limiter(key_func=get_remote_address)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("iniciando ciclo de boot da aplicação via lifespan...")
    try:
        await gerenciador_db.connect()
        # fail fast para evitar subir contêineres "zumbi"
        await gerenciador_db.execucao_segura("SELECT 1")
        logger.info("verificação de sanidade do banco concluída com sucesso. sistema pronto para tráfego.")
    except Exception as e:
        logger.critical(f"falha crítica na inicialização: banco de dados inacessível: {e}")
        sys.exit(1)
    
    yield 
    
    logger.info("encerrando conexões com o banco de dados de forma graciosa...")
    await gerenciador_db.disconnect()

app = FastAPI(
    title="liberstack - radar público de saúde indígena e assistência",
    description="API resiliente orientada ao impacto social e transparência de dados públicos de saúde.",
    version="1.0.0",
    lifespan=lifespan
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS
ALLOWED_ORIGINS = [
    "https://saude.liberstack.com.br",
    "https://indigena.liberstack.com.br"
] if settings.ENV == "production" else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"], 
    allow_headers=["Authorization", "Content-Type"],
)

# middleware global de resiliência para falhas de conexão
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

# endpoints de consumo

@app.get("/v1/indigena/saneamento", tags=["Saúde Indígena"])
@limiter.limit("30/minute")
async def obter_dados_saneamento(request: Request):
    """
    retorna panorama de esgotamento sanitário por DSEI mapeado da tabela correta.
    """
    query = """
        SELECT 
            dsei, 
            polo_base,
            aldeia,
            tipo_esgotamento, 
            quantidade_sistemas as total_sistemas
        FROM staging.esgotamento_sanitario
        ORDER BY dsei, polo_base;
    """
    resultado = await gerenciador_db.execucao_segura(query)
    return {"dados": [dict(r) for r in resultado]}


@app.get("/v1/indigena/agua", tags=["Saúde Indígena"])
@limiter.limit("30/minute")
async def obter_dados_agua(request: Request):
    """
    retorna percentual de atendimento de água e populações vulneráveis por DSEI.
    """
    query = """
        SELECT 
            dsei,
            NULLIF(populacao_total, '')::numeric as populacao_total,
            NULLIF(numero_de_aldeias, '')::integer as total_aldeias,
            NULLIF(pop_total_com_infraestrutura_de_abastecimento, '')::numeric as pop_com_infraestrutura,
            NULLIF(pop_sem_fornecimento, '')::numeric as pop_sem_fornecimento,
            REPLACE(NULLIF(_aldeias_com_infraestrutura, ''), '%', '')::numeric as pct_aldeias_com_infraestrutura,
            REPLACE(NULLIF(satisfatorio__aldeia, ''), '%', '')::numeric as pct_satisfatorio_aldeia
        FROM staging.qualidade_agua
        WHERE dsei IS NOT NULL
        ORDER BY dsei;
    """
    resultado = await gerenciador_db.execucao_segura(query)
    return {"dados": [dict(r) for r in resultado]}


@app.get("/v1/indigena/gestantes", tags=["Saúde Indígena"])
@limiter.limit("30/minute")
async def obter_dados_gestantes(request: Request):
    """
    retorna índices de acompanhamento gestacional e partos direto da tabela dedicada.
    """
    query = """
        SELECT 
            dsei,
            ano_referencia,
            total_gestantes,
            consultas_pre_natal_adequadas,
            parto_hospitalar,
            parto_aldeia,
            ROUND((consultas_pre_natal_adequadas::numeric / NULLIF(total_gestantes, 0)) * 100, 2) as pct_pre_natal_adequado
        FROM staging.acompanhamento_gestacional
        WHERE ano_referencia IS NOT NULL
        ORDER BY ano_referencia DESC, dsei ASC;
    """
    resultado = await gerenciador_db.execucao_segura(query)
    return {"dados": [dict(r) for r in resultado]}


@app.get("/v1/saude", tags=["Monitoramento"])
@limiter.limit("10/minute")
async def health_check(request: Request):
    db_stats = gerenciador_db.get_pool_stats()
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