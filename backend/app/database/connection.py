import asyncio
import asyncpg
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.config.settings import settings

logger = logging.getLogger("liberstack")

class GerenciadorDB:
    def __init__(self):
        self._pool = None

    async def connect(self):
        if not self._pool:
            try:
                # criação da conexão assíncrona 
                self._pool = await asyncpg.create_pool(
                    dsn=f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}",
                    min_size=5,
                    max_size=20,
                    ssl="require",
                    command_timeout=10.0,
                    server_settings={
                        "statement_timeout": "5000",
                        "idle_in_transaction_session_timeout": "10000"
                    }
                )
                logger.info("Pool de conexões Azure Database estabelecido.")
                warmup_conns = [self._pool.acquire() for _ in range(3)]
                conns = await asyncio.gather(*warmup_conns)
                for conn in conns:
                    await self._pool.release(conn)

            except Exception as e:
                logger.critical(f"falha fatal ao criar o pool de conexões Azure: {e}")
                raise e

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
        retry=retry_if_exception_type((asyncpg.OperationalError, asyncpg.PostgresConnectionError)),
        reraise=True
    )
    async def execucao_segura(self, query: str, *args):
        """ evita sobrecarga e controla sessões de conexão"""
        pool = self.get_pool()
        async with pool.acquire() as connection:
            return await connection.fetch(query, *args)

    def get_estatisticas_pool(self) -> dict:
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