from pydantic import BaseModel, Field, field_validator

class DemasHospitalStagingSchema(BaseModel):
    # contrato espelhado com a API de Dados Abertos Federal (DEMAS)
    cnes: str = Field(..., min_length=7, max_length=7, description="Código CNES do estabelecimento")
    nome: str = Field(..., min_length=2, description="Nome da unidade hospitalar")
    municipio: str = Field(..., description="Nome do município do estabelecimento")
    uf: str = Field(..., min_length=2, max_length=2, description="Sigla do Estado")
    bairro: str = Field(default="Não Informado", description="Bairro do estabelecimento")
    
    # dados de insumos/capacidade para mapear na nossa tabela de estoques
    codigo_insumo: str = Field(..., description="Código identificador do insumo, leito ou medicamento")
    nome_insumo: str = Field(..., description="Descrição técnica do insumo/leito")
    quantidade: int = Field(..., gte=0, description="Quantidade disponível constatada")

    @field_validator("cnes")
    @classmethod
    def apenas_numeros_cnes(cls, v: str) -> str:
        if not v.isdigit():
            raise ValueError("o CNES do hospital deve conter apenas números.")
        return v

    @field_validator("nome", "nome_insumo")
    @classmethod
    def normalizar_texto(cls, v: str) -> str:
        return v.strip().upper()