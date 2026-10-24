FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY WM_FO/ .
ENTRYPOINT ["python", "-u", "fo.py"]