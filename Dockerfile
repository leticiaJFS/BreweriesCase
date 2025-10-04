# Usa Python slim para evitar imagem gigante
FROM python:3.11-slim

# Define diretório de trabalho dentro do container
WORKDIR /app

# Copia arquivos de dependências
COPY requirements.txt .

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código do projeto
COPY . .

# Expõe a porta da UI do Luigi
EXPOSE 8082

# Comando default: roda o scheduler do Luigi
CMD ["luigid", "--port", "8082", "--background", "False"]
