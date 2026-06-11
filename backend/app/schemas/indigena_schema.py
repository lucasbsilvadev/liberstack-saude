from pydantic import BaseModel, Field, ConfigDict
from typing import Optional

staging_model_config = ConfigDict(
    extra="ignore",
    populate_by_name=True
)

class EsgotamentoStagingSchema(BaseModel):
    model_config = staging_model_config
    
    dsei: str = Field(..., alias="distrito_sanitario_especial_indigena")
    polo_base: Optional[str] = Field("Não Informado/Agregado", alias="ds_polo_base")
    aldeia: Optional[str] = Field("Não Informado/Agregado", alias="no_aldeia")
    tipo_esgotamento: Optional[str] = Field("Percentuais Agregados", alias="ds_tipo_esgotamento")
    quantidade: int = Field(0, alias="qt_sistemas")


class QualidadeAguaStagingSchema(BaseModel):
    model_config = staging_model_config
    
    dsei: str = Field(..., alias="dsei")
    populacao_total: Optional[str] = Field(None, alias="populacao_total")
    numero_de_aldeias: Optional[str] = Field(None, alias="numero_de_aldeias")
    n_aldeias_pmqai: Optional[str] = Field(None, alias="n_aldeias_pmqai")
    soma_de_infraestrutura_saasac: Optional[str] = Field(None, alias="soma_de_infraestrutura_saasac")
    pop_total_com_infraestrutura_de_abastecimento: Optional[str] = Field(None, alias="pop_total_com_infraestrutura_de_abastecimento")
    pop_total_abastecimento_por_caminhao_pipa: Optional[str] = Field(None, alias="pop_total_abastecimento_por_caminhao_pipa")
    pop_sem_fornecimento: Optional[str] = Field(None, alias="pop_sem_fornecimento")
    
    satisfatorio_pop: Optional[str] = Field(None, alias="satisfatorio_pop")
    requer_manutencao_pop: Optional[str] = Field(None, alias="requer_manutencao_pop")
    requer_substituicao_pop: Optional[str] = Field(None, alias="requer_substituicao_pop")
    sem_info_pop: Optional[str] = Field(None, alias="sem_info_pop")
    
    pct_aldeias_com_infraestrutura: Optional[str] = Field(None, alias="_aldeias_com_infraestrutura")
    pct_aldeia_abastecimento_caminhao_pipa: Optional[str] = Field(None, alias="_aldeia_abastecimento_caminhao_pipa")
    pct_aldeia_sem_fornecimento: Optional[str] = Field(None, alias="_aldeia_sem_fornecimento")
    pct_aldeias_monitoradas_em_relacao_ao_total: Optional[str] = Field(None, alias="_de_aldeias_monitoradas_em_relacao_ao_total")
    pct_aldeias_monitoradas_em_relacao_ao_pmqai_planejado: Optional[str] = Field(None, alias="_de_aldeias_monitoradas_em_relacao_ao_pmqai_planejado")
    
    satisfatorio_aldeia: Optional[str] = Field(None, alias="satisfatorio__aldeia")
    requer_manutencao_aldeia: Optional[str] = Field(None, alias="requer_manutencao__aldeia")
    requer_substituicao_aldeia: Optional[str] = Field(None, alias="requer_substituicao__aldeia")
    sem_info_aldeia: Optional[str] = Field(None, alias="sem_info__aldeia")
    
    media_do_numero_de_aldeias_monitoradas_no_mes_com_analise_dos_6: Optional[str] = Field(None, alias="media_do_numero_de_aldeias_monitoradas_no_mes_com_analise_dos_6")


class GestaoMaternaStagingSchema(BaseModel):
    model_config = staging_model_config
    
    dsei: str = Field(..., alias="gestao_do_dsei")
    ano: int = Field(2024, alias="nu_ano_referencia")
    total_gestantes: int = Field(0, alias="qt_total_gestantes")
    pre_natal_adequado: int = Field(0, alias="qt_consultas_adequadas")
    parto_hospital: int = Field(0, alias="qt_parto_hospitalar")
    parto_aldeia: int = Field(0, alias="qt_parto_domiciliar")