# Sử dụng phiên bản Spark khớp với cluster
FROM spark:3.4.1

USER root
WORKDIR /app

# Cài curl để debug
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt

# --- THAY ĐỔI Ở ĐÂY ---
# Thêm --default-timeout=1000 để tránh lỗi ReadTimeoutError mạng chậm
# Thêm --no-deps để cài nhanh hơn
RUN pip install --default-timeout=1000 --no-cache-dir -r requirements.txt

COPY . /app/
ENV PYTHONPATH=/app

# Tạo thư mục và cấp quyền
RUN mkdir -p /tmp/.ivy2 /opt/spark/logs /opt/spark/work
RUN echo "spark:x:185:185:Spark:/opt/spark:/bin/bash" >> /etc/passwd
RUN chmod -R 777 /opt/spark/logs /opt/spark/work /app /tmp/.ivy2

USER 185