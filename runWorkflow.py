#!/usr/bin/env python3
import os
import sys
import time
import subprocess
from dotenv import load_dotenv
from send_telegram import send_message

# ——— Configuration ———
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    print("ERROR: Please set BOT_TOKEN and CHAT_ID in your environment", file=sys.stderr)
    sys.exit(1)

# List your exact commands here
commands = [
    "python processBDT.py --era UL2016preVFP --outputTag April28",
    "python processBDT.py --era UL2016postVFP --outputTag April28",
    "python processBDT.py --era UL2017 --outputTag April28",
    "python processBDT.py --era UL2018 --outputTag April28",
]

# ——— Run everything ———
timings = []
t0 = time.time()

start_msg = f"🚀 Starting full BDT processing ({len(commands)} jobs)"
print(start_msg)
send_message(BOT_TOKEN, CHAT_ID, start_msg)

for idx, cmd in enumerate(commands, start=1):
    header = f"\n--- [{idx}/{len(commands)}] Starting:\n  {cmd}"
    print(header)
    send_message(BOT_TOKEN, CHAT_ID, f"⏳ {header}")

    t_start = time.time()
    result = subprocess.run(cmd, shell=True)
    duration = int(time.time() - t_start)
    timings.append((cmd, duration))

    if result.returncode != 0:
        fail_msg = f"❌ FAILED after {duration}s:\n  {cmd}"
        print(fail_msg, file=sys.stderr)
        send_message(BOT_TOKEN, CHAT_ID, fail_msg)
        sys.exit(result.returncode)
    else:
        success_msg = f"✅ COMPLETED in {duration}s:\n  {cmd}"
        print(success_msg)
        send_message(BOT_TOKEN, CHAT_ID, success_msg)

# final summary
total = int(time.time() - t0)
summary_lines = [
    "\n🏁 All jobs completed!",
    "",
] + [
    f" • [{i+1}] {cmd} → {dt}s"
    for i, (cmd, dt) in enumerate(timings)
] + [
    f"\nTotal elapsed: {total}s"
]
summary = "\n".join(summary_lines)

print(summary)
send_message(BOT_TOKEN, CHAT_ID, summary)
