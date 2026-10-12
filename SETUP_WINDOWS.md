# Môi trường Day 09 trên Windows

Thư mục làm bài: `C:\Users\nguye\ai20k-day09-2A202602625`.
Repo Public: https://github.com/Munfond/ai20k-day09-2A202602625

## Kết quả setup ngày 12/10/2026

- Docker Desktop hoạt động, Compose v5.5.1; Docker có khoảng 7,6 GiB RAM.
- Dữ liệu: 6/6 file khớp checksum trong `data/MANIFEST.json`.
- `.env`: `LAB_STUDENT_ID=2A202602625`, khớp MSSV trong `REPORT.md`.
- Airflow, MinIO và Postgres đều healthy; scheduler Airflow healthy.
- `step all --reset`: cả 5 bước ĐẠT, 27/27 kiểm tra đạt.
- `dataset_version`: `4af87f527536`; train 718 / val 149 / test 152.
- Bằng chứng: `report/verify.txt`, `report/evidence/run_summary.json`.
- Các file shell đã chuyển sang LF; `.gitattributes` giữ LF khi checkout trên Windows.

## Chạy tiếp trong PowerShell

```powershell
cd C:\Users\nguye\ai20k-day09-2A202602625
python -X utf8 scripts/check_data.py
docker compose --profile orchestrate up -d
docker compose ps
.\scripts\lab.ps1 step 1
.\scripts\lab.ps1 verify
```

`scripts/lab.ps1` tương đương `scripts/lab.sh`: hỗ trợ `step`, `exp`, `verify`, `test`, `run`, `cli`, `dbt`, `shell`.
Ví dụ: `.\scripts\lab.ps1 exp idempotency`.

Chỉ dùng `.\scripts\lab.ps1 step all --reset` khi muốn xóa trạng thái lake và chạy lại từ đầu.
Trước khi chạy pipeline, đợi cả ba dịch vụ báo `healthy`.

- Airflow: http://localhost:8080 — `admin` / `admin`.
- MinIO: http://localhost:9001 — `minioadmin` / `minioadmin`.
- Postgres: localhost:5433 — database `warehouse`, user `lab`, password `lab`.
- Metabase: http://localhost:3000/dashboard/2 — `lab@example.com` / `Day9-lab-pass!`.

## Trạng thái bài lab sau Bước 5

Đã hoàn thành 5 bước, 4 thí nghiệm, 9 unit test, DAG Airflow và bài tập task lỗi/chạy lại; verify cuối đạt 27/27. Metabase có dashboard và câu SQL riêng về tỷ lệ frame giữ lại theo drive; báo cáo và ảnh bằng chứng đã được điền/lưu.
Trip mô phỏng lỗi là `trip-06570`: dùng phần số sau tiền tố `2A` theo lựa chọn của người làm bài (`202602625 % 26 = 17`); MSSV dùng để chia split luôn giữ nguyên `2A202602625`.
Chỉ một frame task được thử lần hai, 25 frame task còn lại không chạy lại; 7 task downstream bị chặn được mở lại để chạy tiếp. DAG cuối có 37 task instance success; dataset_version vẫn là `4af87f527536`.
`LAB_FAIL_ONCE` đã được gỡ và Compose đã áp dụng lại. Khi bật stack sau này, dùng `docker compose --profile orchestrate --profile bi up -d` và đợi Airflow/MinIO/Postgres healthy, Metabase báo `/api/health` = ok.
Không commit `.env`, dữ liệu video/CSV, file zip hoặc thư mục `work/`.
