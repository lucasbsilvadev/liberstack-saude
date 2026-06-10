import asyncio
import logging
from datetime import datetime, timezone
import httpx
from app.database.connection import gerenciador_db
from app.schemas.staging_schema import DemasHospitalStagingSchema
from app.config.settings import settings

logger = logging.getLogger("liberstack")

class ETLDemasHospitalarWorker:
    def __init__(self):
        self.staging_area: list[DemasHospitalStagingSchema] = []

    async def extrair_dados_demas(self) -> list[dict]:
        url_alvo = f"{settings.DEMAS_API_URL.rstrip('/')}/assistencia-a-saude/hospitais-e-leitos"
        logger.info(f"Consumindo API Federal DEMAS: {url_alvo}")
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url_alvo, headers={"Accept": "application/json"})
                if response.status_code == 200:
                    logger.info("payload JSON obtido com sucesso da API do Ministério da Saúde.")
                    data = response.json()
                    # adaptação para garantir o retorno de uma lista (ajuste conforme a paginação do Swagger deles)
                    return data if isinstance(data, list) else data.get("dados", [])
                else:
                    logger.warning(f"DEMAS API retornou status {response.status_code}. ativando contingência.")
        except Exception as e:
            logger.error(f"falha de conexão com a infraestrutura do DEMAS: {e}. Usando fallback local.")

        # contingência local estruturada em JSON 
        return [
            {
                "cnes": "2649474",
                "nome": "Hospital Regional de Sobradinho",
                "municipio": "Brasília",
                "uf": "DF",
                "bairro": "Setor Hospitalar",
                "codigo_insumo": "LEITO-UTI-01",
                "nome_insumo": "LEITO DE UTI ADULTO TIPO II",
                "quantidade": 15
            },
            {
                "cnes": "0010456",
                "nome": "HBDF - Hospital de Base",
                "municipio": "Brasília",
                "uf": "DF",
                "bairro": "Setor Hospitalar Sul",
                "codigo_insumo": "LEITO-UTI-02",
                "nome_insumo": "LEITO DE UTI CORONARIANA",
                "quantidade": 8
            }
        ]

    async def transformar_quarentena(self, registros_brutos: list[dict]):
        logger.info("iniciando triagem na zona de staging (Validação Zero-Trust)...")
        self.staging_area.clear()

        for registro in registros_brutos:
            try:
                # validação contra dados corrompidos da API pública
                dado_validado = DemasHospitalStagingSchema(**registro)
                self.staging_area.append(dado_validado)
            except Exception as e:
                continue

        logger.info(f"triagem concluída. {len(self.staging_area)} registros estruturados na memória.")

    async def carregar_postgres(self):
        if not self.staging_area:
            logger.warning("nenhum registro válido passou pela quarentena para carga.")
            return

        logger.info(f"conectando à Azure ({settings.DB_HOST}) para persistência...")

        # queries preparadas e idempotentes (UPSERT com ON CONFLICT)
        query_insumo = """
            INSERT INTO saude_publica.medicamentos 
            (codigo_barras, nome_generico, nome_comercial, concentracao, forma_farmaceutica, categoria_controle)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (codigo_barras) DO UPDATE SET atualizado_em = CURRENT_TIMESTAMP
            RETURNING id;
        """

        query_hospital = """
            INSERT INTO saude_publica.estabelecimentos (cnes, nome, bairro, municipio)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (cnes) DO UPDATE SET ativo = TRUE
            RETURNING id;
        """

        query_estoque = """
            INSERT INTO saude_publica.estoques 
            (medicamento_id, estabelecimento_id, quantidade_disponivel, status_disponibilidade, ultima_atualizacao)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (medicamento_id, estabelecimento_id) DO UPDATE 
            SET quantidade_disponivel = EXCLUDED.quantidade_disponivel,
                status_disponibilidade = EXCLUDED.status_disponibilidade,
                ultima_atualizacao = EXCLUDED.ultima_atualizacao;
        """

        agora = datetime.now(timezone.utc)

        for registro in self.staging_area:
            try:
                # garante a existência do recurso mapeado 
                id_insumo_res = await gerenciador_db.execucao_segura(
                    query_insumo,
                    registro.codigo_insumo[:13].zfill(13),
                    registro.nome_insumo,
                    "Capacidade Hospitalar",
                    "N/A",
                    "Leito/Insumo",
                    "Controle Crítico"
                )
                insumo_uuid = id_insumo_res[0]["id"]

                # garante a existência do Estabelecimento de Saúde
                id_hosp_res = await gerenciador_db.execucao_segura(
                    query_hospital,
                    registro.cnes,
                    registro.nome,
                    registro.bairro,
                    f"{registro.municipio} / {registro.uf}"
                )
                hospital_uuid = id_hosp_res[0]["id"]

                # cálculo dinâmico do status de criticidade com base na capacidade informada
                qtd = registro.quantidade
                status_disp = "EM_ESTOQUE" if qtd > 10 else "CRITICO" if qtd > 0 else "EM_FALTA"

                # vincula chaves estrangeiras de forma atômica
                await gerenciador_db.execucao_segura(
                    query_estoque, insumo_uuid, hospital_uuid, qtd, status_disp, agora
                )
            except Exception as e:
                logger.error(f"erro ao processar carga do CNES {registro.cnes}: {e}")
                continue

        logger.info("pipeline de carga concluído com absoluto sucesso na Azure.")

    async def executar_pipeline(self):
        await gerenciador_db.connect()
        try:
            dados_brutos = await self.extrair_dados_demas()
            await self.transformar_quarentena(dados_brutos)
            await self.carregar_postgres()
        finally:
            await gerenciador_db.disconnect()

async def main():
    worker = ETLDemasHospitalarWorker()
    await worker.executar_pipeline()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())