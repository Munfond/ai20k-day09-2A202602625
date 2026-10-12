# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `<điền>` |
| MSSV (đúng bằng LAB_STUDENT_ID trong .env) | 2A202602625 |
| GitHub | Munfond |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

> Các số liệu toàn pipeline bên dưới thuộc lượt chạy thử setup đã đạt 27/27. Sau khi thực hành Bước 1 với `--reset` và hoàn tất Bước 2–3, lake hiện có Bronze và Silver telemetry/frames; sẽ chạy lại các bước tiếp theo và cập nhật kết quả cuối khi hoàn tất.

- `dataset_version` của bạn: `4af87f527536`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 718 / 149 / 152
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A dùng `ts_utc` dạng ISO UTC, ví dụ `2025-10-07T08:00:00.014Z`; B dùng `timestamp_ms` dạng epoch mili giây, ví dụ `1759933801509`.
A có `speed_mps` theo m/s (mẫu đầu `1.213`), B có `spd_kmh` theo km/h (mẫu đầu `55.11`); đây là hai chuyến khác nhau, không phải hai phép đo cùng một thời điểm.
Cột hướng của A là `heading_deg`, của B là `hdg`; GPS của A là `lat`/`lon`, của B là `latitude`/`longitude`.
Bàn đạp A là `gas`/`brake` theo tỷ lệ 0–1 (mẫu `gas=0.028`), B là `throttle_pct`/`brake_pct` theo phần trăm (mẫu `throttle_pct=2.0`); ngoài ra A dùng `acc_x`/`acc_y`/`steer`, B dùng `ax`/`ay`/`steering`.
Bronze giữ hai bảng riêng vì tên cột, đơn vị và cách biểu diễn thời gian khác nhau. Các cột nguồn đều là `string`, kể cả 30 giá trị tốc độ rỗng `""` ở mỗi nguồn; các cột metadata `_row_no` và `_ingested_at` có kiểu riêng.
Giữ nguyên giá trị giúp truy vết, đọc lại dữ liệu và tránh mất dòng lỗi do ép kiểu sớm; Silver mới chuyển đơn vị, parse timestamp, hiệu chỉnh đồng hồ B lệch 1.500 ms và phân loại lỗi vào quarantine.
Sau nạp lặp, A vẫn có 3.021 dòng, B 3.234 dòng; mỗi bảng telemetry chỉ có một commit Delta (version 0). `video_files` vẫn có 2 dòng/2 SHA256 khác nhau; hai commit của bảng video ứng với hai file được nạp lần đầu, lượt nạp lại không tạo commit mới.
Bằng chứng: `report/evidence/bronze_step1.txt`, `bronze_history_a.json`, `bronze_observations.json`; MinIO có `bronze/video/{drive_a,drive_b}/raw.mp4` và `bronze/delta/raw_telemetry_a/_delta_log/00000000000000000000.json`.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Trong staging A, `ts_utc` được cast sang `timestamptz`, tốc độ vốn là m/s được chuyển từ chuỗi sang `double`. Staging B đổi tên cột về schema A, chia `spd_kmh` cho 3,6 và chia bàn đạp phần trăm cho 100; `nullif(..., '')` cùng `try_cast` chuyển tốc độ trống/không parse được thành NULL.
Thời gian B được tính bằng `to_timestamp((timestamp_ms - offset_ms) / 1000.0)`, với offset đọc từ `bronze.video_files`. B lệch 1.500 ms nên được trừ 1,5 giây; timestamp đầu sau chỉnh là `2025-10-08 14:30:00.009000+00:00`, cách video bắt đầu `14:30:00+00:00` chỉ 9 ms, không phải lệch 1.509 ms như trước chỉnh.
Kết quả thực tế: tổng 6.149 dòng sạch và 106 dòng quarantine; các lý do theo từng nguồn như sau:

| Drive | Dòng nhận | Dòng sạch | duplicate | speed_missing | speed_out_of_range | Tổng rejects |
|---|---:|---:|---:|---:|---:|---:|
| drive_a | 3.021 | 2.968 | 20 | 30 | 3 | 53 |
| drive_b | 3.234 | 3.181 | 20 | 30 | 3 | 53 |

Đối soát cho thấy `2.968 + 53 = 3.021` và `3.181 + 53 = 3.234`, không mất dòng. Tốc độ tối đa sạch là 19,818 m/s ở A và khoảng 27,831 m/s ở B, đều trong ngưỡng 0–70 m/s; module 2 đạt 7/7 kiểm tra.
Ví dụ rejects thực tế: A tại `08:00:54.184Z` là `duplicate`, A tại `08:01:59.189Z` là `speed_missing`; cả hai drive có 3 dòng tốc độ 250 m/s bị `speed_out_of_range`. Rejects giữ `drive_id`, `ts`, lý do, tốc độ, `_batch_id` và `_row_no`, còn Bronze giữ bản gốc để truy vết; xóa im lặng sẽ làm mất bằng chứng lỗi và khó đối soát nguồn.
Bằng chứng: `report/evidence/silver_telemetry_step2.txt`, `silver_rejects_sample.txt` và `silver_telemetry_observations.json`.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`schema.yml` đặt `max_gap_ms` ngưỡng 1.000 ms theo từng drive và `severity: warn`: mất tín hiệu là khiếm khuyết cần theo dõi nhưng được lab cho phép tiếp tục. Dữ liệu sạch của tôi có hai khoảng trống vượt ngưỡng, A 3.073 ms và B 3.124 ms; vì vậy một test WARN ứng với 2 kết quả vi phạm, không phải chỉ một dòng.
`in_range` kiểm tra tốc độ 0–70 m/s và dùng mức error mặc định: tốc độ phi lý không được đi vào dataset. Chạy thường có `PASS=6 WARN=1 ERROR=0 SKIP=0`; cảnh báo khoảng trống không khiến bước 2 thất bại.
Thí nghiệm nới riêng luật phân loại `max_speed_mps` lên 1.000, nhưng test vẫn giữ ngưỡng 70. Cả 6 dòng tốc độ 250 m/s (3 dòng mỗi drive) lọt vào bảng sạch; `in_range_silver_telemetry_speed_mps__70__0` bắt đủ 6 dòng, cho `PASS=5 WARN=1 ERROR=1 SKIP=0` và `gate_failed=true`.
Script đã khôi phục luật 70, kết quả trở về `PASS=6 WARN=1 ERROR=0 SKIP=0`; kiểm tra lại bảng sạch không còn tốc độ ngoài 0–70 m/s hoặc NULL. Model và cấu hình dbt không bị sửa tay.
Thí nghiệm cho thấy luật làm sạch và cổng kiểm tra độc lập: khi luật làm sạch bị nới sai, test error vẫn phát hiện dữ liệu xấu để bước kiểm tra thất bại và orchestration chặn các task phía sau; warn chỉ báo vấn đề được chấp nhận trong lab.
Bằng chứng: `report/evidence/break_gate.json`, `break_gate_log.txt` và kết quả test sau khôi phục trong `silver_telemetry_observations.json`.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Tôi chạy lại `trip-00003`, có 60 frame. Trước và sau hai lần trích lại, toàn bảng `silver.frames` vẫn có 1.246 dòng, dấu vân tay đều là `6c7480dfde7e`; `idempotency.json` ghi `unchanged=true`.
Mô phỏng append vào bảng thử cho cùng trip hai lần tạo 120 dòng, bằng 2 × 60: dữ liệu bị nhân đôi dù nội dung frame không đổi.
Pipeline dùng Delta `overwrite` với predicate `trip_id = 'trip-00003'` để thay đúng nhóm dòng của trip, giữ dữ liệu các trip khác; thay đổi bảng được công bố qua một commit giao dịch, tránh khoảng trống giữa thao tác xóa và ghi riêng rẽ.
Nếu lần ghi chưa commit bị lỗi, phiên bản đã commit trước đó vẫn là trạng thái có thể đọc; khi chạy lại, trip được thay thế thay vì append thêm. Ảnh JPG được ghi lại vào cùng key, còn tính giao dịch nói trên áp dụng cho bảng Delta, không phải một giao dịch chung cho mọi ảnh và bảng.
Idempotent ở đây là số dòng và nội dung được lấy dấu vân tay không đổi; timestamp `_processed_at` không nằm trong phép băm và lịch sử Delta vẫn có thêm hai commit, đưa bảng từ version 25 lên 27.
Bước 3 đạt 6/6 kiểm tra, dbt test `PASS=8 WARN=0 ERROR=0`; 9/9 unit test đều OK. Bằng chứng: `idempotency.json`, `idempotency_log.txt`, `silver_frames_step3.txt` và `silver_frames_unit_tests.txt` trong `report/evidence/`.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen có độ sáng trung bình `<12`; khung tối có `12 ≤ brightness <40` và vẫn có thể chứa thông tin cảnh quay. Silver giữ frame cùng các cờ; Gold lọc đen, mờ (`blur_score <40` khi không đen), gần trùng hoặc mất đồng bộ, còn cờ tối không tự gây loại bỏ.
Thống kê thực tế: A có 602 frame, 6 đen, 9 mờ, 4 tối, 149 gần trùng, 7 mất đồng bộ; B có 644 frame, 4 đen, 0 mờ, 61 tối, 53 gần trùng, 7 mất đồng bộ. Các cờ có thể chồng nhau; 4 frame tối ở A và 58/61 frame tối ở B đáp ứng các điều kiện để vào Gold, 3 frame tối còn lại ở B vướng cờ khác.
Tôi đối chiếu ảnh MinIO: `drive_a/01001.jpg` đen hoàn toàn, brightness 0 và sync gap 18 ms; `01998.jpg` bị nhòe rõ, blur score 2,485 và gap 16 ms; `00545.jpg` có bóng tối nhưng vẫn thấy đường/cây, brightness 39,647, blur score 947,195 và gap 91 ms, nên được giữ.
Frame bình thường `00005.jpg` có gap 92 ms. Ghép ASOF chọn telemetry cùng drive gần nhất ở trước hoặc đúng thời điểm frame; gap trung bình A/B là 66/73 ms, tối đa 2.884/3.112 ms, và gap >150 ms hoặc không có bản ghi trước frame được gắn `sync_lost`.
Tôi đồng ý giữ cảnh tối vì đó là điều kiện thực tế mà mô hình cần học, đồng thời vẫn loại nếu ảnh mờ/trùng hoặc ghép telemetry không đạt. Tôi chọn ngưỡng dHash 2 cho lab: ngưỡng 0 chỉ bắt hash giống hệt, còn 6 tăng số frame bị coi là trùng thêm 214 ở A và 191 ở B so với 2, có nguy cơ loại cảnh còn khác biệt.
Ở tốc độ lấy mẫu 2 fps cố định, xe chạy chậm/đứng yên thường làm hai frame liên tiếp giống nhau hơn; xe chạy nhanh thường làm cảnh thay đổi nhiều hơn. Ngưỡng cần xét cùng tốc độ, chuyển động cảnh và fps; dHash ít khác không tự chứng minh camera bị đứng hình. Ngưỡng 2 là lựa chọn cân bằng cho lab này, chưa phải tối ưu đã được đo bằng chất lượng mô hình.
Bằng chứng: `report/evidence/threshold.json`, `threshold_log.txt`, `silver_frames_observations.json`; ảnh đối chiếu nằm trong `report/screenshots/silver_frame_{normal,black,blurry,dark_kept}.png`.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
