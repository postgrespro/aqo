import re
import pandas as pd
from pathlib import Path
import csv

# Các từ khoá mở đầu 1 câu SQL phổ biến (để phát hiện ranh giới)
SQL_STARTERS = r"(select|with|insert|update|delete|create|alter|drop|truncate|merge|grant|revoke|exec|execute)"
SELECT_STARTERS = r"(select|with)"

start_any_re    = re.compile(rf"^\s*{SQL_STARTERS}\b", re.I)
start_select_re = re.compile(rf"^\s*{SELECT_STARTERS}\b", re.I)

# Tìm EXEC ... '...SQL...' (kể cả xuống dòng, có thể là N'...')
# Ví dụ: EXEC spExecuteSQL 'SELECT ...', '500000', @log=0
EXEC_RE = re.compile(r"""(?ix)
    \bexec(?:ute)?       # EXEC hoặc EXECUTE
    [^\S\r\n]+[a-zA-Z0-9_\.]+   # tên proc
    [^\S\r\n]*           # khoảng trắng
    (N)?'                # mở quote (có thể N')
    (                     # nhóm 2: nội dung bên trong quote
        (?: '' | [^'] )* # xử lý escape '' và mọi ký tự khác ngoài '
    )
    '                    # đóng quote
""", re.DOTALL)

def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _split_keep_selects(sql_text: str):
    """
    Tách nhiều câu trong 1 chuỗi (ngăn bằng ; hoặc xuống dòng),
    chỉ trả về câu bắt đầu bằng SELECT/WITH.
    """
    # tách sơ bộ theo ';' nhưng vẫn an toàn vì string bên trong EXEC đã được tách riêng
    parts = [p.strip() for p in re.split(r";\s*", sql_text) if p.strip()]
    out = []
    for p in parts:
        # nếu không có ;, vẫn giữ nguyên đoạn p
        # lọc chỉ SELECT/WITH
        if re.match(rf"^\s*{SELECT_STARTERS}\b", p, re.I):
            out.append(_normalize_spaces(p))
    return out

def extract_selects_from_file(path: str):
    txt = Path(path).read_text(encoding="utf-8", errors="ignore")
    txt = txt.replace("\r\n", "\n").replace("\r", "\n").lstrip("\ufeff")

    selects = []

    # 1) Bóc tất cả SQL nằm trong EXEC '...'
    exec_select_chunks = []
    for m in EXEC_RE.finditer(txt):
        raw = m.group(2)
        # unescape '' -> '
        inner = raw.replace("''", "'")
        exec_select_chunks.extend(_split_keep_selects(inner))

    selects.extend(exec_select_chunks)

    # 2) Xử lý phần còn lại bên ngoài EXEC: gom theo ranh giới câu lệnh
    #    Để tránh double-count, loại tạm thời nội dung đã match EXEC (thay bằng khoảng trắng)
    txt_no_exec = EXEC_RE.sub(" ", txt)

    lines = [ln.rstrip() for ln in txt_no_exec.split("\n")]
    # bỏ header "statement" nếu có
    if lines and lines[0].strip().lower() == "statement":
        lines = lines[1:]

    buf = []
    buf_is_select = False

    def flush():
        nonlocal buf, buf_is_select
        if buf and buf_is_select:
            selects.append(_normalize_spaces(" ".join(buf)))
        buf, buf_is_select = [], False

    for ln in lines:
        stripped = ln.strip()
        if not stripped:
            flush()
            continue

        if start_any_re.match(stripped):
            # gặp đầu câu mới -> chốt câu cũ
            flush()
            buf = [ln]
            buf_is_select = bool(start_select_re.match(stripped))
        else:
            if buf:
                buf.append(ln)
            # nếu chưa ở trong 1 câu thì bỏ qua

        if stripped.endswith(";"):
            flush()

    flush()

    # 3) Loại rỗng + trùng
    unique = []
    seen = set()
    for s in selects:
        if s and s.lower().startswith(("select", "with")):
            key = s.lower()
            if key not in seen:
                seen.add(key)
                unique.append(s)
    return unique

# ---- DÙNG ----
# Ví dụ với file "queries.csv" hoặc file .txt bạn đang có
select_only = extract_selects_from_file("pure_sql_dataset.csv")

# Xuất ra CSV 1 cột, quote toàn bộ để pandas đọc chắc chắn
df = pd.DataFrame({"statement": select_only})
df.to_csv("select_only.csv", index=False, quoting=csv.QUOTE_ALL)

print(f"Đã trích {len(df)} câu SELECT/WITH và lưu vào select_only.csv")
