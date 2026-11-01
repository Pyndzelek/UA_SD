FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY WM_Central/ .
RUN mkdir -p /app/data
ENTRYPOINT ["python", "-u", "WM_Central.py"]
