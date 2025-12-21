# 📦 Crawl - Tiền xử lý & Tải dữ liệu thời tiết

Thư mục này chứa các script và notebook dùng để tải, hợp nhất và làm sạch dữ liệu thời tiết thô từ nhiều nguồn khác nhau.

## ✨ Chức năng chính
- Tải dữ liệu thời tiết lịch sử từ các file CSV.
- Hợp nhất các chỉ số: nhiệt độ, độ ẩm, áp suất, mô tả thời tiết, hướng gió, tốc độ gió.
- Chuyển đổi định dạng, làm sạch và chuẩn hóa dữ liệu cho các pipeline ETL và ML.

## 📁 Cấu trúc thư mục
```
crawl/
├── download_data_weather.ipynb  # Notebook xử lý, hợp nhất dữ liệu thời tiết
└── README.md                    # Tài liệu này
```

## 🚀 Hướng dẫn sử dụng
1. Mở notebook `download_data_weather.ipynb` bằng Jupyter hoặc VSCode.
2. Chạy từng cell để tải, hợp nhất và làm sạch dữ liệu.
3. Kết quả sẽ được lưu ra file `data_weather.csv` dùng cho các bước tiếp theo.

## ⚙️ Lưu ý cấu hình
- Đảm bảo các file dữ liệu gốc (CSV) đã có trong thư mục chỉ định.
- Có thể chỉnh sửa đường dẫn, tên file, hoặc các bước xử lý cho phù hợp nguồn dữ liệu thực tế.

## 📝 Ghi chú
- Dữ liệu sau khi xử lý sẽ được dùng cho pipeline Spark ETL và huấn luyện mô hình ML.
- Có thể mở rộng thêm các bước crawl từ API hoặc các nguồn dữ liệu khác.

---
Tiền xử lý dữ liệu là bước quan trọng để đảm bảo chất lượng dự báo thời tiết!
