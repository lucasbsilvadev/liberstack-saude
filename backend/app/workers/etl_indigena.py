import asyncio
import logging
from datetime import datetime, timezone
import httpx
from app.database.connection import gerenciador_db
from app.config.settings import settings
from app.schemas.indigena_schema import (
    EsgotamentoStagingSchema, 
    QualidadeAguaStagingSchema, 
    GestaoMaternaStagingSchema
)

logger = logging.getLogger("liberstack")

class ETLSaudeIndigenaWorker:
    def __init__(self):
        self.base_url = settings.DEMAS_API_URL.rstrip('/')
        self.limit = 200 

    async def _coletar_paginado(self, endpoint: str) -> list[dict]:
        """gerencia looping de paginação real na API DEMAS com logs visuais de progresso"""
        todos_registros = []
        offset = 0
        pagina = 1
        
        async with httpx.AsyncClient(timeout=20.0) as client:
            while True:
                url = f"{self.base_url}{endpoint}"
                logger.info(f"pagina {pagina}: solicitando {endpoint} (offset: {offset}, limit: {self.limit})")
                
                try:
                    response = await client.get(url, params={"limit": self.limit, "offset": offset})
                    
                    # debug: status code e headers
                    logger.debug(f"status code: {response.status_code}")
                    logger.debug(f"content-type: {response.headers.get('content-type', 'unknown')}")
                    
                    if response.status_code != 200:
                        logger.error(f"erro http {response.status_code} no endpoint {endpoint} na pagina {pagina}.")
                        # debug: corpo do erro se existir
                        if response.status_code == 404:
                            logger.error(f"endpoint nao encontrado: verifique o caminho {endpoint}")
                        break
                        
                    payload = response.json()
                    
                    # debug: tipo da resposta
                    logger.debug(f"tipo do payload: {type(payload).__name__}")
                    
                    if isinstance(payload, list):
                        dados_bloco = payload
                    elif isinstance(payload, dict):
                        dados_bloco = next((v for v in payload.values() if isinstance(v, list)), [])
                        logger.debug(f"chaves do dicionario: {list(payload.keys())}")
                    else:
                        dados_bloco = []
                        logger.warning(f"formato de resposta inesperado: {type(payload)}")
                    
                    logger.info(f"pagina {pagina}: sucesso. retornados {len(dados_bloco)} registros.")
                    
                    if not dados_bloco:
                        logger.info(f"fim da paginacao para {endpoint}.")
                        break
                        
                    # debug: primeiro registro para inspecao
                    if pagina == 1 and dados_bloco:
                        logger.debug(f"amostra do primeiro registro: {list(dados_bloco[0].keys()) if isinstance(dados_bloco[0], dict) else 'nao e dicionario'}")
                        
                    todos_registros.extend(dados_bloco)
                    
                    if len(dados_bloco) < self.limit:
                        logger.info(f"bloco menor que o limite ({len(dados_bloco)} < {self.limit}). paginacao finalizada.")
                        break
                        
                    offset += self.limit
                    pagina += 1
                    await asyncio.sleep(0.3)
                    
                except httpx.TimeoutException:
                    logger.error(f"timeout ao ler {endpoint} no offset {offset}. pulando.")
                    break
                except Exception as e:
                    logger.critical(f"falha na comunicacao em {endpoint}: {e}")
                    break
                    
        logger.info(f"total de registros coletados para {endpoint}: {len(todos_registros)}")
        return todos_registros

    async def pipeline_saneamento(self):
        logger.info("iniciando pipeline de saneamento")
        dados_brutos = await self._coletar_paginado("/saude-indigena/sasisus-esgotamento-sanitario")
        
        if not dados_brutos:
            logger.warning("nenhum dado bruto recuperado para saneamento")
            return

        validados = []
        erros_validacao = 0
        
        for idx, reg in enumerate(dados_brutos):
            try:
                validados.append(EsgotamentoStagingSchema(**reg))
            except Exception as e:
                erros_validacao += 1
                if erros_validacao <= 3:
                    logger.warning(f"erro validacao registro {idx}: {e}")
                    logger.debug(f"dados problematicos: {reg}")
                continue 
        
        logger.info(f"validados: {len(validados)} registros, erros: {erros_validacao}")
        
        if not validados:
            logger.warning("nenhum registro valido para saneamento")
            return
                
        logger.info(f"gravando {len(validados)} registros em staging.esgotamento_sanitario")
        
        query = """
            INSERT INTO staging.esgotamento_sanitario 
            (dsei, polo_base, aldeia, tipo_esgotamento, quantidade_sistemas)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (dsei, polo_base, aldeia, tipo_esgotamento) 
            DO UPDATE SET quantidade_sistemas = EXCLUDED.quantidade_sistemas, atualizado_em = CURRENT_TIMESTAMP;
        """
        
        inseridos = 0
        for v in validados:
            try:
                await gerenciador_db.execucao_segura(query, v.dsei, v.polo_base, v.aldeia, v.tipo_esgotamento, v.quantidade)
                inseridos += 1
            except Exception as e:
                logger.error(f"falha ao inserir registro {v.dsei}: {e}")
                
        logger.info(f"pipeline saneamento finalizado. inseridos: {inseridos}/{len(validados)}")

    async def pipeline_agua(self):
        logger.info("iniciando pipeline de qualidade da agua")
        dados_brutos = await self._coletar_paginado("/saude-indigena/planilha-de-fornecimento-e-monitoramento-da-qualidade-da-agua-acesso-a-agua")
        
        if not dados_brutos:
            logger.warning("nenhum dado bruto recuperado para agua")
            return

        validados = []
        erros_validacao = 0
        
        for idx, reg in enumerate(dados_brutos):
            try:
                validados.append(QualidadeAguaStagingSchema(**reg))
            except Exception as e:
                erros_validacao += 1
                if erros_validacao <= 3:
                    logger.warning(f"erro validacao registro {idx}: {e}")
                    logger.debug(f"dados problematicos: {reg}")
                continue
                
        logger.info(f"validados: {len(validados)} registros, erros: {erros_validacao}")
        
        if not validados:
            logger.warning("nenhum registro valido para agua")
            return
                
        logger.info(f"gravando {len(validados)} registros em staging.qualidade_agua")
        
        query = """
            INSERT INTO staging.qualidade_agua 
            (
                dsei, populacao_total, numero_de_aldeias, n_aldeias_pmqai, soma_de_infraestrutura_saasac,
                pop_total_com_infraestrutura_de_abastecimento, pop_total_abastecimento_por_caminhao_pipa, pop_sem_fornecimento,
                satisfatorio_pop, requer_manutencao_pop, requer_substituicao_pop, sem_info_pop,
                _aldeias_com_infraestrutura, _aldeia_abastecimento_caminhao_pipa, _aldeia_sem_fornecimento,
                satisfatorio__aldeia, requer_manutencao__aldeia, requer_substituicao__aldeia, sem_info__aldeia,
                _de_aldeias_monitoradas_em_relacao_ao_total, _de_aldeias_monitoradas_em_relacao_ao_pmqai_planejado,
                media_do_numero_de_aldeias_monitoradas_no_mes_com_analise_dos_6
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
            ON CONFLICT (dsei) 
            DO UPDATE SET 
                populacao_total = EXCLUDED.populacao_total,
                numero_de_aldeias = EXCLUDED.numero_de_aldeias,
                n_aldeias_pmqai = EXCLUDED.n_aldeias_pmqai,
                soma_de_infraestrutura_saasac = EXCLUDED.soma_de_infraestrutura_saasac,
                pop_total_com_infraestrutura_de_abastecimento = EXCLUDED.pop_total_com_infraestrutura_de_abastecimento,
                pop_total_abastecimento_por_caminhao_pipa = EXCLUDED.pop_total_abastecimento_por_caminhao_pipa,
                pop_sem_fornecimento = EXCLUDED.pop_sem_fornecimento,
                satisfatorio_pop = EXCLUDED.satisfatorio_pop,
                requer_manutencao_pop = EXCLUDED.requer_manutencao_pop,
                requer_substituicao_pop = EXCLUDED.requer_substituicao_pop,
                sem_info_pop = EXCLUDED.sem_info_pop,
                _aldeias_com_infraestrutura = EXCLUDED._aldeias_com_infraestrutura,
                _aldeia_abastecimento_caminhao_pipa = EXCLUDED._aldeia_abastecimento_caminhao_pipa,
                _aldeia_sem_fornecimento = EXCLUDED._aldeia_sem_fornecimento,
                satisfatorio__aldeia = EXCLUDED.satisfatorio__aldeia,
                requer_manutencao__aldeia = EXCLUDED.requer_manutencao__aldeia,
                requer_substituicao__aldeia = EXCLUDED.requer_substituicao__aldeia,
                sem_info__aldeia = EXCLUDED.sem_info__aldeia,
                _de_aldeias_monitoradas_em_relacao_ao_total = EXCLUDED._de_aldeias_monitoradas_em_relacao_ao_total,
                _de_aldeias_monitoradas_em_relacao_ao_pmqai_planejado = EXCLUDED._de_aldeias_monitoradas_em_relacao_ao_pmqai_planejado,
                media_do_numero_de_aldeias_monitoradas_no_mes_com_analise_dos_6 = EXCLUDED.media_do_numero_de_aldeias_monitoradas_no_mes_com_analise_dos_6,
                extracted_at = CURRENT_TIMESTAMP;
        """
        
        inseridos = 0
        for v in validados:
            try:
                await gerenciador_db.execucao_segura(
                    query, 
                    v.dsei, v.populacao_total, v.numero_de_aldeias, v.n_aldeias_pmqai, v.soma_de_infraestrutura_saasac,
                    v.pop_total_com_infraestrutura_de_abastecimento, v.pop_total_abastecimento_por_caminhao_pipa, v.pop_sem_fornecimento,
                    v.satisfatorio_pop, v.requer_manutencao_pop, v.requer_substituicao_pop, v.sem_info_pop,
                    v.pct_aldeias_com_infraestrutura, v.pct_aldeia_abastecimento_caminhao_pipa, v.pct_aldeia_sem_fornecimento,
                    v.satisfatorio_aldeia, v.requer_manutencao_aldeia, v.requer_substituicao_aldeia, v.sem_info_aldeia,
                    v.pct_aldeias_monitoradas_em_relacao_ao_total, v.pct_aldeias_monitoradas_em_relacao_ao_pmqai_planejado,
                    v.media_do_numero_de_aldeias_monitoradas_no_mes_com_analise_dos_6
                )
                inseridos += 1
            except Exception as e:
                logger.error(f"falha ao inserir registro {v.dsei}: {e}")
                
        logger.info(f"pipeline agua finalizado. inseridos: {inseridos}/{len(validados)}")
            
    async def pipeline_gestacional(self):
        logger.info("iniciando pipeline de acompanhamento gestacional")
        
        # endpoint corrigido com 'siasi' (um 'i' a mais)
        dados_brutos = await self._coletar_paginado("/saude-indigena/siasi-acompanhamento-gestacional")
        
        if not dados_brutos:
            logger.warning("nenhum dado bruto recuperado para gestantes")
            return

        logger.info(f"total de registros brutos para gestantes: {len(dados_brutos)}")
        
        # debug: mostrar estrutura do primeiro registro
        if dados_brutos and len(dados_brutos) > 0:
            logger.debug(f"chaves do primeiro registro: {list(dados_brutos[0].keys())}")
        
        validados = []
        erros_validacao = 0
        
        for idx, reg in enumerate(dados_brutos):
            try:
                validados.append(GestaoMaternaStagingSchema(**reg))
            except Exception as e:
                erros_validacao += 1
                if erros_validacao <= 3:
                    logger.warning(f"erro validacao registro {idx}: {e}")
                    logger.debug(f"dados problematicos: {reg}")
                continue
                
        logger.info(f"validados: {len(validados)} registros, erros: {erros_validacao}")
        
        if not validados:
            logger.warning("nenhum registro valido para gestantes")
            return
                
        logger.info(f"gravando {len(validados)} registros em staging.acompanhamento_gestacional")
        
        query = """
            INSERT INTO staging.acompanhamento_gestacional 
            (dsei, ano_referencia, total_gestantes, consultas_pre_natal_adequadas, parto_hospitalar, parto_aldeia)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (dsei, ano_referencia) 
            DO UPDATE SET total_gestantes = EXCLUDED.total_gestantes, 
                          consultas_pre_natal_adequadas = EXCLUDED.consultas_pre_natal_adequadas,
                          parto_hospitalar = EXCLUDED.parto_hospitalar,
                          parto_aldeia = EXCLUDED.parto_aldeia,
                          atualizado_em = CURRENT_TIMESTAMP;
        """
        
        inseridos = 0
        for v in validados:
            try:
                await gerenciador_db.execucao_segura(query, v.dsei, v.ano, v.total_gestantes, v.pre_natal_adequado, v.parto_hospital, v.parto_aldeia)
                inseridos += 1
            except Exception as e:
                logger.error(f"falha ao inserir registro {v.dsei}: {e}")
                
        logger.info(f"pipeline gestacional finalizado. inseridos: {inseridos}/{len(validados)}")

    async def executar_todos_pipelines(self):
        logger.info("iniciando varredura de dados de saude indigena")
        await gerenciador_db.connect()
        try:
            await self.pipeline_saneamento()
            await self.pipeline_agua()
            await self.pipeline_gestacional()
            logger.info("carga completa executada (pipeline gestacional desabilitado para debug)")
        except Exception as e:
            logger.error(f"erro na execucao dos pipelines: {e}")
        finally:
            await gerenciador_db.disconnect()