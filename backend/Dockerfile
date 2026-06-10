FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=America/Sao_Paulo

# define diretório de trabalho
WORKDIR /app

# cria usuario comum 
RUN addgroup --system liberstack && adduser --system --group liberstack

# copia apenas dependências 
COPY requirements.txt .

# instalação das libs
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# copia a aplicação
COPY . .

# transfere a posse dos arquivos para o usuário seguro
RUN chown -R liberstack:liberstack /app

USER liberstack

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]