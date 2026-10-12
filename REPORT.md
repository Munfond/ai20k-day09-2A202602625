# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Đức Anh |
| MSSV (đúng bằng LAB_STUDENT_ID trong .env) | 2A202602625 |
| GitHub | Munfond |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-06570` |

## Kết quả cuối

> Đã hoàn thành cả năm bước, DAG Airflow thành công và bài tập phục hồi task lỗi đạt 27/27. Dataset, split và số dòng sau phục hồi khớp lượt DAG chuẩn; `LAB_FAIL_ONCE` đã được gỡ khỏi `.env` và áp dụng lại Docker Compose.

Trước khi nộp, tôi chạy lại `step all --reset` với MSSV thật `2A202602625` và cấu hình mặc định. Lượt cuối đạt cả năm bước và 27/27; `run_summary.json` được sinh lại, vẫn có phiên bản và split như dưới đây. Log nằm ở `report/evidence/final_full_run.txt`; ảnh Airflow và các thí nghiệm giữ bằng chứng của các lượt thực hiện trước đó.

- `dataset_version` của bạn: `4af87f527536`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 718 / 149 / 152
- Số trip train / val / test: 18 / 4 / 4; 26 trip đều chỉ thuộc một split.
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

Thí nghiệm dùng 1.019 frame đủ chất lượng thuộc 26 trip. Chia theo frame bằng hash của `frame_id` để mô phỏng split 70/15/15 làm 26/26 trip nằm ở nhiều split; cả 26 trip đều có frame ở train lẫn test.
Chia theo trip của Gold cho 0 trip bị chia cắt; train có 718 frame/18 trip, val 149/4 và test 152/4. Mỗi drive đều có 9 trip train, 2 val và 2 test.
Các frame gần nhau trong cùng cảnh thường tương tự về đường, vật thể và điều kiện quay. Nếu đưa chúng vào cả train và test, mô hình có thể tận dụng cảnh đã gặp thay vì khái quát sang chuyến mới, nên điểm test có nguy cơ cao một cách giả tạo.
Thí nghiệm này đo rò rỉ trip, không huấn luyện mô hình hay đo mức tăng accuracy; chia theo trip loại được kiểu rò rỉ đang kiểm tra, nhưng không tự chứng minh mọi nguồn rò rỉ khác đã biến mất.
Bằng chứng: `report/evidence/leakage.json`, `leakage_log.txt`; danh sách 26 trip và split tương ứng nằm trong `gold_observations.json`.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Version của tôi là `4af87f527536`. Lần chạy Bước 4 và lần gọi `export-dataset` tiếp theo đều cho version này, cùng train/val/test = 718/149/152 và cùng SHA256 manifest Parquet `1947e4f7a0fd2dd0f8432021b723d14904ead90d57d079f1cf10830e0d255a7f`.
Cụ thể, mã export sắp theo `frame_id`, ghép từng cặp `frame_id|split`, băm SHA256 rồi lấy 12 ký tự đầu. Vì phụ thuộc tập frame và cách chia split, cùng đầu vào và cùng MSSV cho cùng version; ngày giờ hay số đếm sẽ đổi theo lần chạy dù dataset không đổi.
MSSV `2A202602625` được trộn vào `md5(trip_id || LAB_STUDENT_ID)` để xếp trip trong từng drive trước khi gán split. MSSV khác có thể làm trip đổi split và đổi version, dù cùng video và telemetry; tôi giữ nguyên MSSV thật, không thực hiện thử đổi MSSV trong bước này.
Version này không băm toàn bộ byte ảnh, mọi cột telemetry hay lineage. Hai manifest JSON có `created_at` khác nhau nhưng version không đổi; cần dùng thêm checksum, lineage và nguồn đã lưu để kiểm tra đầy đủ dataset, không coi 12 ký tự version là bằng chứng duy nhất.
Bằng chứng so sánh: `report/evidence/gold_manifest_first.json`, `gold_manifest_second.json`, `gold_export_again.txt` và trường `repeat_export` trong `gold_observations.json`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest cuối tại `gold/datasets/4af87f527536/manifest.json` ghi: `bronze.raw_telemetry_a=0`, `bronze.raw_telemetry_b=0`, `bronze.trips_raw=0`, `bronze.video_files=1`; `silver.frames=25`, `silver.telemetry=0`, `silver.frames_flagged=0`, `silver.frame_telemetry=0`.
Đó là telemetry thô của hai drive và metadata trip/video ở Bronze, cùng các frame, telemetry sạch, cờ chất lượng và kết quả ghép event time ở Silver. Bản manifest lưu ở Bước 4 có frames version 27 do hai lần ghi lại trip ở thí nghiệm idempotency; reset trước DAG tạo lịch sử mới, nên bản cuối có version 25 cho 26 lần ghi thành công, nhưng dataset_version và nội dung frame vẫn không đổi.
`input_video_sha256` xác định chính xác hai file video nguồn:

```text
drive_a: 636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa
drive_b: bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc
```

Manifest còn ghi luật loại đen/mờ/gần trùng/không sync_ok, split theo trip phân tầng theo drive và checksum manifest Parquet. Sau 3 tháng, cần giữ manifest, dữ liệu nguồn đúng checksum, các phiên bản Delta kèm log/file dữ liệu, MSSV `2A202602625`, code Git và cấu hình/ngưỡng/thư viện để đọc lại hoặc dựng lại đúng kết quả.
Lineage giúp truy ngược phiên bản bảng và video; manifest hiện không ghi trực tiếp MSSV, commit code hay toàn bộ cấu hình môi trường, nên không thể chỉ dựa vào manifest để tái tạo mọi chi tiết. Bản cuối nằm trong `report/evidence/manifest_after_recovery.json`; hai bản `gold_manifest_first.json`/`gold_manifest_second.json` lưu bằng chứng export lặp ở Bước 4.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

MSSV thật là `2A202602625`. Vì lệnh mẫu `int(MSSV)` không nhận chữ, tôi dùng phần số sau tiền tố `2A` theo lựa chọn đã xác nhận: `202602625 % 26 = 17`, ứng với `trip-06570` trong danh sách trip đã sắp xếp; MSSV dùng cho split vẫn giữ nguyên cả chuỗi `2A202602625`.
Lượt DAG chuẩn có 37 task instance thành công, gồm 26 `silver_frames`. Sau khi bật `LAB_FAIL_ONCE=trip-06570`, reset rồi trigger lại, chỉ `silver_frames[17]` failed; 7 task downstream là `upstream_failed`, 29 task còn lại đã success. Log có `RuntimeError: simulated crash while extracting trip-06570; run it again`; lỗi mô phỏng xuất hiện trước khi ghi dữ liệu của trip.
Tôi chọn Clear and Retry trên đúng instance `[17]`. Hộp thoại bật Downstream, nên phạm vi phục hồi là 1 task lỗi và 7 task đang bị chặn, không có task success nào; không clear cả nhóm 26 frame task hoặc chạy lại cả DAG từ đầu. Các task downstream được mở lại trạng thái để chạy lần đầu sau khi trip lỗi được xử lý.
Sau phục hồi, cùng run ID `manual__2026-10-12T10:42:27.791776+00:00` có 37/37 task success. Chỉ map index 17 có try number 2; 25 frame task khác vẫn try 1, và cả 29 task đã success trước clear giữ nguyên start/end time và try number.
Trip được phục hồi có 45 frame; toàn Silver có 1.246 dòng và 1.246 frame_id duy nhất, Gold có 1.019 dòng và 1.019 frame_id duy nhất. Dấu vân tay frame trước/sau đều `6c7480dfde7e`, dataset_version đều `4af87f527536`, train/val/test vẫn 718/149/152; không nhân đôi dữ liệu.
Verify sau phục hồi đạt 27/27; tôi gỡ `LAB_FAIL_ONCE` khỏi `.env` và áp dụng lại Compose. Warehouse có đủ 6 mart; gallery có 1.019 dòng khớp Gold.
Bằng chứng: `airflow_graph.png`, `fail_task_log.png`, `airflow_clear_scope.png`, `airflow_retry_graph.png` trong `report/screenshots/`; `airflow_failure_before_clear.json`, `airflow_recovered.json`, `airflow_recovery_summary.json`, `dataset_baseline.json`, `dataset_after_recovery.json`, `airflow_fail_task_log.txt` trong `report/evidence/`, cùng `report/verify.txt` và `run_summary.json`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Nếu mỗi xe có khối lượng tương đương một drive của lab, 500 xe là khoảng 250 lần số nguồn hiện tại. Tôi dự đoán ba nút nghẽn dưới đây từ cấu hình đang chạy; đây là phân tích kiến trúc, chưa phải benchmark 500 xe.
Thứ nhất là trích video trên worker Airflow: lab dùng LocalExecutor, parallelism 2 và giới hạn một task `silver_frames` chạy cùng lúc trong mỗi DAG run. Cần tách scheduler khỏi worker, dùng hàng đợi/worker phân tán và mở rộng số worker xử lý FFmpeg theo trip, với giới hạn CPU/RAM và ghi idempotent theo trip.
Thứ hai là ELT dbt/DuckDB trên một file `lakehouse.duckdb` ở ổ local trong container giới hạn RAM khoảng 2,5 GB. Khi scan/join và nhiều job cùng tăng, cần engine SQL phân tán đọc lakehouse và adapter dbt phù hợp, tổ chức partition/compaction để giảm đọc dư; dbt vẫn giữ vai trò định nghĩa model và test, không tự trở thành engine phân tán.
Thứ ba là lớp lưu trữ MinIO một instance, giới hạn 768 MB, nhận video, rất nhiều JPG nhỏ và các file Delta. Cần object storage có khả năng mở rộng/dự phòng, tối ưu số request/file nhỏ, quản lý vòng đời và dung lượng; không đưa toàn bộ dữ liệu gốc vào warehouse/dashboard.
Các thay đổi tương ứng với việc tách storage–compute–orchestration trong kiến trúc dữ liệu, mở rộng phần ELT/kiểm tra chất lượng và xử lý video theo partition. Vẫn phải giữ lineage, cổng error/warn và split theo trip; tăng máy nhưng mất các bảo đảm này sẽ tạo dataset lớn mà khó tin cậy.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Câu hỏi: “Drive nào giữ được tỷ lệ frame cao hơn sau cổng chất lượng?” Tôi thêm câu SQL sau vào dashboard Metabase, dựa trên `marts.mart_trip_summary` để đếm frame bị loại đúng một lần thay vì cộng các cờ có thể chồng nhau:

```sql
select drive_id,
       sum(frames) as frames_total,
       sum(frames_kept) as frames_kept,
       round(100.0 * sum(frames_kept) / nullif(sum(frames), 0), 2) as kept_pct,
       round(100.0 * (sum(frames) - sum(frames_kept)) / nullif(sum(frames), 0), 2) as dropped_pct
from marts.mart_trip_summary
group by drive_id
order by drive_id;
```

| Drive | Tổng frame | Giữ lại | Giữ (%) | Loại (%) |
|---|---:|---:|---:|---:|
| drive_a | 602 | 436 | 72,43 | 27,57 |
| drive_b | 644 | 583 | 90,53 | 9,47 |

B có tỷ lệ giữ cao hơn A 18,10 điểm phần trăm. Số liệu bước 3 cho thấy A có nhiều frame gần trùng hơn (149 so với 53) và có thêm frame mờ, phù hợp với chênh lệch này; vì cờ có thể chồng nhau, bảng tỷ lệ riêng không chứng minh đóng góp độc lập của từng lý do.
Question: `http://localhost:3000/question/46`; dashboard: `http://localhost:3000/dashboard/2`. SQL và kết quả trả về được lưu trong `report/evidence/metabase_question.json`.

## Bonus · Thắt ngưỡng đồng bộ frame–telemetry

Tôi thử giảm `sync_tol_ms` từ **150 ms xuống 75 ms**, giữ nguyên MSSV, dữ liệu, dHash, blur và luật split. Mục tiêu là đo lượng ảnh phải hy sinh khi đòi telemetry gần thời điểm frame hơn. Tôi chạy lại hai model liên quan và test bằng tham số tạm thời, không sửa mã hoặc cấu hình nguồn:

```bash
scripts/lab.sh dbt run --select silver_frame_telemetry gold_dataset_frames --vars '{"sync_tol_ms": 75}'
scripts/lab.sh dbt test --select silver_frame_telemetry gold_dataset_frames --vars '{"sync_tol_ms": 75}'
# Khôi phục bằng cách bỏ tham số override:
scripts/lab.sh dbt run --select silver_frame_telemetry gold_dataset_frames
scripts/lab.sh dbt test --select silver_frame_telemetry gold_dataset_frames
scripts/lab.sh verify
```

| Chỉ số | Trước: 150 ms | Thử: 75 ms | Khôi phục: 150 ms |
|---|---:|---:|---:|
| A: frame mất đồng bộ / tổng | 7 / 602 | 272 / 602 | 7 / 602 |
| B: frame mất đồng bộ / tổng | 7 / 644 | 328 / 644 | 7 / 644 |
| A: frame được giữ vào Gold | 436 | 242 | 436 |
| B: frame được giữ vào Gold | 583 | 296 | 583 |
| Tổng frame Gold | 1.019 | 538 | 1.019 |
| Train / val / test | 718 / 149 / 152 | 376 / 67 / 95 | 718 / 149 / 152 |
| dbt test hai model | — | PASS 7, WARN 1, ERROR 0 | PASS 8, WARN 0, ERROR 0 |

Giảm ngưỡng làm số frame mất đồng bộ tăng từ 14 lên 600, tức thêm 586 frame bị gắn cờ; Gold giảm 481 frame (47,20%). Hai số khác nhau vì một số frame mất đồng bộ mới đã bị loại bởi cờ chất lượng khác. B mất thêm 287 ảnh Gold, A mất thêm 194 ảnh. Với phép ghép ASOF, đây là khoảng cách tới mẫu telemetry gần nhất **ở trước hoặc đúng thời điểm frame**; giảm ngưỡng không sửa timestamp hay chọn một phép ghép mới.

Test `max_share_silver_frame_telemetry_sync_lost__0_05` phát cảnh báo vì tỷ lệ mất đồng bộ toàn bộ frame tăng lên 600/1.246 = 48,15%, vượt 5%. Đây là một WARN cho test tổng tỷ lệ, không phải 600 test lỗi. Các test Gold vẫn PASS vì Gold đã lọc frame không đồng bộ; dataset hợp lệ theo luật nhưng nhỏ hơn nhiều. Thí nghiệm cho thấy đánh đổi giữa độ sát thời gian và lượng dữ liệu, chưa chứng minh mô hình học tốt hơn ở ngưỡng nào. Tôi giữ 150 ms cho bài nộp chính.

Dấu phiên bản tính từ các cặp `frame_id|split` trong DuckDB đổi từ `4af87f527536` sang `9a8901ad883f` trong thí nghiệm và trở về `4af87f527536` sau khôi phục. Bản 75 ms chỉ được tính và kiểm tra trong DuckDB, không export/publish vào lake hoặc warehouse; manifest và dashboard bài chính được giữ ở bản mặc định. File `dbt/dbt_project.yml` vẫn là bản gốc với 150 ms. Sau khôi phục, toàn bộ số đếm và phiên bản khớp trước thí nghiệm, verify đạt **27/27**.

Bằng chứng: `report/evidence/bonus_sync_tolerance.json`, `bonus_sync_75ms_dbt.txt`, `bonus_sync_restored_dbt.txt` và `bonus_sync_verify.txt`. Đây là một thí nghiệm riêng cho mục điểm cộng trong RUBRIC.md; mức điểm thực tế do giảng viên chấm, tối đa +5 và tổng không vượt 100.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điểm đáng chú ý là frame tối vẫn có thể rất nét và dùng được: B có 61 frame tối nhưng 58 frame vẫn đủ điều kiện vào Gold; loại tất cả cảnh tối sẽ làm dataset thiếu những tình huống thực tế.
Một điều cần tìm hiểu thêm là ngưỡng dHash/blur thay đổi thế nào khi FPS, tốc độ xe và camera thay đổi; số đếm của lab chưa chứng minh ngưỡng 2 là tối ưu cho chất lượng mô hình.
Pipeline chưa có test kiểm tra GPS nhảy bất thường hoặc sự nhất quán vật lý giữa tốc độ và quãng đường từ GPS: các tọa độ parse được và tốc độ nằm trong ngưỡng vẫn có thể mâu thuẫn. Tôi sẽ bổ sung kiểm tra theo event time và ghi lý do quarantine thay vì xóa dòng.
