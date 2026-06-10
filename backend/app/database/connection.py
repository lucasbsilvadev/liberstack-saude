import asyncio
import asyncpg
import ssl
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from asyncpg import exceptions as asyncpg_errors
from app.config.settings import settings

logger = logging.getLogger("liberstack")

class GerenciadorDB:
    def __init__(self):
        self._pool = None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((
            asyncpg_errors.InterfaceError,
            ConnectionRefusedError,
            OSError
        )),
        reraise=True
    )
    async def connect(self):
        if self._pool:
            return
      
        logger.info("tentando estabelecer o pool de conexões com o Azure Database...")
        
        # contexto ssl

        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        self._pool = await asyncpg.create_pool(
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            host=settings.DB_HOST,
            port=int(settings.DB_PORT),
            database=settings.DB_NAME,
            ssl=ctx,  
            command_timeout=10.0,
            server_settings={
                "statement_timeout": "5000",
                "idle_in_transaction_session_timeout": "10000"
            }
        )
        logger.info("pool de conexões Azure Database estabelecido com sucesso.")
        
        warmup_conns = [self._pool.acquire() for _ in range(3)]
        conns = await asyncio.gather(*warmup_conns)
        for conn in conns:
            await self._pool.release(conn)

    async def disconnect(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("pool de conexões encerrado graciosamente.")

    def get_pool(self):
        if not self._pool:
            raise RuntimeError("GerenciadorDB não foi inicializado ou o pool de conexões está fechado.")
        return self._pool

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        retry=retry_if_exception_type((
            asyncpg_errors.InterfaceError, 
            asyncpg_errors.PostgresError,   
            OSError                        
        )),
        reraise=True
    )
    async def execucao_segura(self, query: str, *args):
        pool = self.get_pool()
        async with pool.acquire() as connection:
            return await connection.fetch(query, *args)

    def get_pool_stats(self) -> dict:
        if not self._pool:
            return {"status": "uninitialized"}
            
        total = self._pool.get_size()
        idle = self._pool.get_idle_size()
           
        return {
            "status": "healthy" if idle > 0 else "congested",
            "total_connections": total,
            "idle_connections": idle,
            "used_connections": total - idle
        }

gerenciador_db = GerenciadorDB()