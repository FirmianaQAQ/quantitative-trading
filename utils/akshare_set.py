from pathlib import Path

cookie_str = (Path(__file__).resolve().parent.parent / "data" / "cookie.txt").read_text(encoding="utf-8").strip()
eastmoney_headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "cookie": cookie_str,
}