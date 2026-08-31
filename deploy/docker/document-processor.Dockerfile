FROM python:3.12-slim
WORKDIR /app
COPY services/document-processor/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY services/document-processor/ ./
EXPOSE 8000
CMD ["python", "main.py"]
