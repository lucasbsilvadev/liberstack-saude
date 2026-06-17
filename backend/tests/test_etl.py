import pytest
import asyncpg
from app.workers.etl_infosaude import main as rodar_worker_real
from config.settings import settings  # Ajuste conforme seu arquivo de configurações

@pytest.mark.asyncio
async def test_pipeline_etl_real_no_database():
    await rodar_worker_real()
    
    # conexão com DB
    conn = await asyncpg.connect(settings.DATABASE_URL)
    
    try:
        # busca se a tabela de staging ou produção possui registros 
        total_registros = await conn.fetchval("SELECT COUNT(*) FROM staging_producao_hospitalar")
        
        # validação DML do banco
        assert total_registros > 0, "worker executou corretamente, porém nenhum dado foi encontrado"
        print(f"\n sucesso! {total_registros} registros foram extraídos e salvos no banco Docker.")
        
    finally:
        await conn.close()
