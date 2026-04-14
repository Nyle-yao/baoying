#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


def _run_url() -> str:
    server = os.getenv("GITHUB_SERVER_URL", "https://github.com")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    run_id = os.getenv("GITHUB_RUN_ID", "")
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return ""


def _load_meta() -> dict[str, str]:
    meta_path = Path("exports/addsub/dashboard_meta.json")
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _build_content(event: str, extra: str | None, include_meta: bool) -> str:
    status_map = {
        "start": "开始执行",
        "success": "执行成功",
        "failure": "执行失败",
    }
    status = status_map.get(event, event)

    workflow = os.getenv("GITHUB_WORKFLOW", "update-and-deploy")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    actor = os.getenv("GITHUB_ACTOR", "")
    ref_name = os.getenv("GITHUB_REF_NAME", "")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_url = _run_url()

    lines = [
        f"[看板自动更新]{status}",
        f"仓库: {repo}",
        f"工作流: {workflow}",
        f"分支: {ref_name}",
        f"触发人: {actor}",
        f"时间: {now}",
    ]
    if run_url:
        lines.append(f"运行详情: {run_url}")

    if include_meta:
        meta = _load_meta()
        if meta:
            latest_date = meta.get("latest_date", "-")
            crawl_at = meta.get("crawl_at", "-")
            source_update_latest = meta.get("source_update_latest", "-")
            rows = meta.get("latest_date_total_rows", "-")
            lines.extend([
                f"最新统计日期: {latest_date}",
                f"该日总行数: {rows}",
                f"抓取时间: {crawl_at}",
                f"源站更新时间: {source_update_latest}",
            ])

    if extra:
        lines.append(f"备注: {extra}")

    return "\n".join(lines)


def _notify_wecom(content: str) -> tuple[bool, str]:
    webhook = os.getenv("WECOM_WEBHOOK_URL", "").strip()
    if not webhook:
        return False, "WECOM_WEBHOOK_URL 未配置"

    payload = {
        "msgtype": "text",
        "text": {
            "content": content,
        },
    }
    req = Request(
        webhook,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        try:
            ret = json.loads(body)
            ok = ret.get("errcode") == 0
            return ok, body
        except Exception:
            return True, body
    except URLError as e:
        return False, f"{e}"


def _notify_email(subject: str, content: str) -> tuple[bool, str]:
    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "465"))
    user = os.getenv("SMTP_USER", "").strip()
    pwd = os.getenv("SMTP_PASSWORD", "").strip()
    to_raw = os.getenv("SMTP_TO", "").strip()
    from_addr = os.getenv("SMTP_FROM", user).strip()

    if not (host and user and pwd and to_raw):
        return False, "SMTP_* 未完整配置"

    to_list = [x.strip() for x in to_raw.split(",") if x.strip()]
    if not to_list:
        return False, "SMTP_TO 为空"

    msg = MIMEText(content, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_list)

    try:
        if port == 465:
            with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(), timeout=20) as s:
                s.login(user, pwd)
                s.sendmail(from_addr, to_list, msg.as_string())
        else:
            with smtplib.SMTP(host, port, timeout=20) as s:
                s.ehlo()
                s.starttls(context=ssl.create_default_context())
                s.ehlo()
                s.login(user, pwd)
                s.sendmail(from_addr, to_list, msg.as_string())
        return True, "sent"
    except Exception as e:
        return False, str(e)


def main() -> int:
    parser = argparse.ArgumentParser(description="Notify workflow status via WeCom or Email")
    parser.add_argument("--event", default="custom", help="start/success/failure/custom")
    parser.add_argument("--extra", default="", help="Extra text")
    parser.add_argument("--include-meta", action="store_true", help="Append dashboard meta when available")
    args = parser.parse_args()

    content = _build_content(args.event, args.extra, args.include_meta)
    subject = f"[看板自动更新]{args.event.upper()}"

    ok_wecom, msg_wecom = _notify_wecom(content)
    ok_email, msg_email = _notify_email(subject, content)

    print("content:\n" + content)
    print(f"wecom_ok={ok_wecom} detail={msg_wecom}")
    print(f"email_ok={ok_email} detail={msg_email}")

    # 不因通知失败阻断主流程
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
