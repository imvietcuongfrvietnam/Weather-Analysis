FROM spark:3.4.1

USER root

WORKDIR /app

# Cài thư viện
RUN pip install --no-cache-dir numpy pandas scikit-learn

# Copy source code
COPY . /app/

# --- KHẮC PHỤC LỖI NULL POINTER (QUAN TRỌNG) ---
# 1. Thêm user 185 vào file /etc/passwd để Java biết tên user này là "spark"
RUN echo "spark:x:185:185:Spark:/opt/spark:/bin/bash" >> /etc/passwd

# 2. Tạo thư mục và cấp quyền (như cũ)
RUN mkdir -p /opt/spark/logs /opt/spark/work && \
    chmod -R 777 /opt/spark/logs /opt/spark/work /app

# Chuyển về user 185
USER 185