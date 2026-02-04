import zmq
import threading
import time
import json
import uuid
import os
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import glob
import hashlib

OP_BIND = "tcp://*:6000"

OP_ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_NEW_DIR = os.path.join(OP_ROOT, "config_new")

LOG_DIR = "log"
os.makedirs(LOG_DIR, exist_ok=True)
ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = os.path.join(LOG_DIR, f"{ts}_Operator.log")

logger = logging.getLogger("operator")
logger.setLevel(logging.INFO)
logger.handlers.clear()

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s"))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(console_handler)


def cprint(msg: str):
    logger.info(msg)


def flog(msg: str):
    file_handler.emit(logging.LogRecord(
        name="operator.file",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None,
    ))


@dataclass
class Reply:
    ok: bool
    data: dict


class Operator:

    def __init__(self, bind_addr: str = OP_BIND):
        self.bind_addr = bind_addr
        self.ctx = zmq.Context.instance()
        self.sock = self.ctx.socket(zmq.ROUTER)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.bind(bind_addr)
        self._stop = threading.Event()
        self.components: Dict[str, dict] = {}
        self.rename_pending: Dict[str, dict] = {}
        self._replies: Dict[str, Reply] = {}
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self.rx_thread = threading.Thread(target=self._rx_loop, name="OperatorRx", daemon=True)
        self.console_debug = False

    def start(self):
        cprint(f"[operator] bind {self.bind_addr}")
        cprint(f"[operator] root = {OP_ROOT}")
        cprint(f"[operator] config_new = {CONFIG_NEW_DIR}")
        cprint(f"[operator] log file: {LOG_FILE}")
        self.rx_thread.start()
        try:
            self._cli_loop()
        finally:
            self.close()

    def close(self):
        if self._stop.is_set():
            return
        self._stop.set()
        try:
            if self.rx_thread.is_alive():
                self.rx_thread.join(timeout=1.0)
        except Exception:
            pass
        try:
            self.sock.close(0)
        except Exception:
            pass
        try:
            self.ctx.term()
        except Exception:
            pass

    @staticmethod
    def _split_frames(frames) -> Tuple[Optional[bytes], Optional[bytes]]:
        if len(frames) == 2:
            return frames[0], frames[1]
        if len(frames) >= 3 and frames[1] == b"":
            return frames[0], frames[2]
        return None, None

    @staticmethod
    def _base_ident(ident: str) -> str:
        return ident[:-4] if ident.endswith("_out") else ident

    def _mark_seen(self, cid: str):
        if cid not in self.components:
            self.components[cid] = {"last_seen": time.time(), "info": {}, "status": "ONLINE"}
        else:
            self.components[cid]["last_seen"] = time.time()

    def _rx_loop(self):
        poller = zmq.Poller()
        poller.register(self.sock, zmq.POLLIN)

        while not self._stop.is_set():
            try:
                socks = dict(poller.poll(200))
                if self.sock not in socks:
                    continue
                frames = self.sock.recv_multipart()
            except zmq.error.ZMQError:
                if self._stop.is_set():
                    break
                continue

            ident_b, payload = self._split_frames(frames)
            if ident_b is None:
                flog(f"DROP frames={frames!r}")
                continue

            ident = ident_b.decode("utf-8", errors="ignore")
            base = self._base_ident(ident)
            self._mark_seen(base)

            try:
                msg = json.loads(payload.decode("utf-8"))
            except Exception:
                cprint(f"[bad] json from {base}")
                flog(f"BAD_JSON from={ident} payload={payload!r}")
                continue

            mtype = msg.get("type")

            if mtype == "hello":
                info = msg.get("info", {}) or {}
                self.components[base]["info"] = info
                # keep existing status if RENAMING, else ONLINE
                if self.components[base].get("status") != "RENAMING":
                    self.components[base]["status"] = "ONLINE"

                cprint(f"[operator] registered: {base} (info={info})")
                flog(f"HELLO from={ident} msg={msg}")

                old_to_remove = None
                for old_id, meta in self.rename_pending.items():
                    if meta.get("new_id") == base:
                        old_to_remove = old_id
                        break
                if old_to_remove:
                    cprint(f"[operator] rename completed: {old_to_remove} -> {base}")
                    flog(f"RENAME_DONE old={old_to_remove} new={base} meta={self.rename_pending.get(old_to_remove)}")
                    self.rename_pending.pop(old_to_remove, None)
                    if old_to_remove in self.components:
                        self.components.pop(old_to_remove, None)

                ack = {"type": "hello_ack", "ts": time.time()}
                self.sock.send_multipart([ident_b, json.dumps(ack).encode("utf-8")])
                continue

            if mtype == "reply":
                req_id = msg.get("request_id", "")
                ok = bool(msg.get("ok", False))
                with self._cv:
                    self._replies[req_id] = Reply(ok=ok, data=msg)
                    self._cv.notify_all()
                flog(f"REPLY_FULL from={ident} req_id={req_id} msg={msg}")
                continue

            if mtype == "event":
                ev = msg.get("event")
                if ev == "id_change":
                    old_id = msg.get("old_id")
                    new_id = msg.get("new_id")
                    if isinstance(old_id, str) and isinstance(new_id, str) and old_id:
                        self.rename_pending[old_id] = {"new_id": new_id, "ts": time.time()}
                        if old_id in self.components:
                            self.components[old_id]["status"] = "RENAMING"
                        cprint(f"[operator] rename pending: {old_id} -> {new_id}")
                    flog(f"EVENT_ID_CHANGE from={ident} msg={msg}")
                else:
                    flog(f"EVENT_FULL from={ident} msg={msg}")
                continue

            flog(f"UNKNOWN from={ident} msg={msg}")

    def _send_cmd_wait(self, target: str, cmd: str, args: Optional[dict], timeout_s: float) -> Reply:
        if target not in self.components:
            if target in self.rename_pending:
                return Reply(ok=False, data={
                    "error": f"target '{target}' is renaming to '{self.rename_pending[target]['new_id']}' (try new id)"
                })
            return Reply(ok=False, data={"error": f"target '{target}' not registered"})

        req_id = str(uuid.uuid4())
        msg = {
            "type": "cmd",
            "cmd": cmd,
            "target": target,
            "request_id": req_id,
            "args": args or {},
            "ts": time.time(),
        }
        raw = json.dumps(msg).encode("utf-8")
        flog(f"CMD_FULL to={target} req_id={req_id} cmd={cmd}")
        try:
            self.sock.send_multipart([target.encode("utf-8"), raw])
        except zmq.error.ZMQError as e:
            return Reply(ok=False, data={"error": f"send failed: {e}", "request_id": req_id})
        deadline = time.time() + timeout_s
        with self._cv:
            while time.time() < deadline:
                if req_id in self._replies:
                    return self._replies.pop(req_id)
                self._cv.wait(timeout=0.1)

        return Reply(ok=False, data={"error": "timeout waiting reply", "request_id": req_id})

    def _print_help(self):
        cprint("\nCommands:")
        cprint("  list")
        cprint("  <id>_start | <id>_stop | <id>_restart | <id>_status")
        cprint("  <id>_pushcfg [file.yaml]   (send ./config_new/<file.yaml> or newest yaml)")
        cprint("  <cmd> --debug              (show full reply on console for THIS command only)")
        cprint("  debug on|off               (toggle persistent console debug)")
        cprint("  help")
        cprint("  quit\n")

    @staticmethod
    def _parse_line(line: str):
        parts = line.strip().split()
        if not parts:
            return "", False
        debug_once = False
        if "--debug" in parts:
            debug_once = True
            parts = [p for p in parts if p != "--debug"]
        core = " ".join(parts)
        return core, debug_once

    @staticmethod
    def _pick_newest_yaml(dir_path: str) -> Optional[str]:
        pats = [os.path.join(dir_path, "*.yaml"), os.path.join(dir_path, "*.yml")]
        files = []
        for p in pats:
            files.extend(glob.glob(p))
        if not files:
            return None
        files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return files[0]

    def _cli_loop(self):
        self._print_help()

        while not self._stop.is_set():
            try:
                raw_line = input("operator> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not raw_line:
                continue

            line, debug_once = self._parse_line(raw_line)

            if line in ("quit", "exit"):
                cprint("Operator offline")
                break
            if line == "help":
                self._print_help()
                continue

            if line == "debug on":
                self.console_debug = True
                cprint("[operator] console debug = ON")
                continue
            if line == "debug off":
                self.console_debug = False
                cprint("[operator] console debug = OFF")
                continue

            if line == "list":
                if not self.components:
                    cprint("(no components)")
                else:
                    for cid, meta in sorted(self.components.items()):
                        t = time.strftime("%H:%M:%S", time.localtime(meta.get("last_seen", 0)))
                        status = meta.get("status", "ONLINE")
                        hint = ""
                        if cid in self.rename_pending:
                            hint = f"  -> {self.rename_pending[cid]['new_id']}"
                        cprint(f"- {cid} [{status}] (last_seen={t}){hint}")
                continue

            tokens = line.split()
            head = tokens[0]

            if "_" not in head:
                cprint("bad command. use: <id>_status or list/help")
                continue

            target, action = head.rsplit("_", 1)

            if action == "pushcfg":
                os.makedirs(CONFIG_NEW_DIR, exist_ok=True)

                if len(tokens) >= 2:
                    name = tokens[1]
                    src_path = name if os.path.isabs(name) else os.path.join(CONFIG_NEW_DIR, name)
                else:
                    src_path = self._pick_newest_yaml(CONFIG_NEW_DIR)

                if not src_path or not os.path.exists(src_path):
                    cprint(f"[ng] no yaml found. put new yaml under: {CONFIG_NEW_DIR}")
                    continue

                try:
                    text = open(src_path, "r", encoding="utf-8").read()
                except Exception as e:
                    cprint(f"[ng] cannot read: {src_path} ({e})")
                    continue

                b = text.encode("utf-8")
                sha = hashlib.sha256(b).hexdigest()
                show_full_console = debug_once or self.console_debug
                cprint(f"CMD -> {target} push_config (from {os.path.basename(src_path)})")
                rep = self._send_cmd_wait(
                    target,
                    "push_config",
                    args={
                        "content": text,
                        "src_name": os.path.basename(src_path),
                        "src_sha256": sha,
                        "src_bytes": len(b),
                        "sent_at": time.time(),
                    },
                    timeout_s=5.0
                )

                if show_full_console:
                    cprint(json.dumps(rep.data, ensure_ascii=False))
                    cprint(json.dumps({
                        "sent_file": os.path.basename(src_path),
                        "sent_bytes": len(b),
                        "sent_sha256": sha,
                    }, ensure_ascii=False))
                else:
                    if rep.ok:
                        cprint(f"[ok] pushed {os.path.basename(src_path)} bytes={len(b)} sha256={sha[:12]}…")
                    else:
                        cprint(f"[ng] {rep.data}")

                flog(f"CLI_PUSHCFG raw={raw_line!r} src_path={src_path!r} bytes={len(b)} sha256={sha} ok={rep.ok} reply={rep.data}")
                continue

            # ---- normal actions ----
            action_map = {
                "start": "start",
                "stop": "stop",
                "restart": "restart",
                "status": "status",
            }
            if action not in action_map:
                cprint("unknown action. use start/stop/restart/status/pushcfg")
                continue

            show_full_console = debug_once or self.console_debug

            cprint(f"CMD -> {target} {action_map[action]}")
            rep = self._send_cmd_wait(target, action_map[action], args={}, timeout_s=8.0)

            if show_full_console:
                cprint(json.dumps(rep.data, ensure_ascii=False))
            else:
                if rep.ok:
                    cprint(f"[ok] state={rep.data.get('state')} msg={rep.data.get('message')}")
                else:
                    cprint(f"[ng] {rep.data}")

            flog(f"CLI_LINE raw={raw_line!r} parsed={line!r} debug_once={debug_once} debug_persist={self.console_debug}")
            flog(f"CLI_RESULT ok={rep.ok} reply={rep.data}")


if __name__ == "__main__":
    Operator().start()
