from pydantic import BaseModel, Field, field_validator

class ProducaoHospitalarStagingSchema(BaseModel):
    # contrato de dados real para produção hospitalar da SES-DF
    ano_competencia: str = Field(..., min_length=4, max_length=4, description="Ano do procedimento")
    mes_competencia: str = Field(..., min_length=2, max_length=2, description="mês do procedimento")
    cnes_hospital: str = Field(..., min_length=7, max_length=7, description="CNES do Hospital do DF")
    nome_hospital: str = Field(..., min_length=2, description="Nome da unidade de saúde")
    procedimento_codigo: str = Field(..., description="Código do procedimento/cirurgia no SUS")
    procedimento_nome: str = Field(..., description="Descrição do procedimento cirúrgico ou clínico")
    quantidade_realizada: int = Field(..., gte=0, description="Volume físico de atendimentos executados")

    @field_validator("cnes_hospital")
    @classmethod
    def apenas_numeros_cnes(cls, v: str) -> str:
        if not v.isdigit():
            raise ValueError("o CNES do hospital deve conter apenas números.")
        return v

    @field_validator("procedimento_nome")
    @classmethod
    def normalizar_texto(cls, v: str) -> str:
        return v.strip().upper()