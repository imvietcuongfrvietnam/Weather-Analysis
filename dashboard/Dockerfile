# Sử dụng Python nhẹ
FROM python:3.9-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# --- SỬA LỖI QUAN TRỌNG ---
# Cài đặt curl để dùng cho Healthcheck (và các utils khác nếu cần)
# && rm -rf... để xóa cache apt giúp image nhẹ đi
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 1. Copy requirements vào trước (để tận dụng Docker layer cache)
COPY requirements.txt .

# 2. Cài đặt thư viện
# Thêm --no-cache-dir để không lưu cache pip, giảm dung lượng image
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy toàn bộ code trong folder dashboard vào container
COPY . .

# Mở port mặc định của Streamlit
EXPOSE 8501

# Healthcheck chuẩn
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Lệnh chạy ứng dụng
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]