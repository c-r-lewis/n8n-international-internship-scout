FROM ghcr.io/astral-sh/uv:python3.12-alpine
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1
COPY . /app
RUN uv pip install --system fastmcp pandas httpx python-dotenv uvicorn tavily-python
EXPOSE 3000
CMD ["python", "main.py"]