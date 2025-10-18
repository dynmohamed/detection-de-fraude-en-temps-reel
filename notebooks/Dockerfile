FROM python:3.10-slim
WORKDIR /app
COPY . /app 
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8889
CMD ["jupyter", "lab", "--ip=0.0.0.0","--port=8889","--no-browser","--allow-root"]