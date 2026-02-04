import zmq
import threading
import time
import logging
import yaml
import sys
import signal
import os
import json
from datetime import datetime
from queue import Queue, Empty

CONFIG_FILE = "./config/middleware_config.yaml"
CONFIG_DIR = "./config"
OP_ADDR = "tcp://127.0.0.1:6000"

log_dir = "log"
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logfile_path = os.path.join(log_dir, f"{timestamp}_Middleware.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(threadName)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(logfile_path, encoding="utf-8"),
              logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("middleware")


def load_config():
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f)


class Middleware:
    def __init__(self):
        os.makedirs(CONFIG_DIR, exist_ok=True)

        self.context = zmq.Context()
        self.stop_event = threading.Event()

        self.data_stop_event = threading.Event()
        self.cmd_stop_event = threading.Event()

        self.downstream_clients = {}

        self.input_pusher_url = "inproc://input_pusher"
        self.output_to_main_url = "inproc://output_to_main"
        self.main_to_output_url = "inproc://main_to_output"

        self.cmd_to_main_url = "inproc://cmd_to_main"

        self.cmd_out_queue: "Queue[bytes]" = Queue(maxsize=1000)

        # state
        self.data_enabled = True
        self.state = "RUNNING"

        self._apply_loaded_config(load_config(), initial=True)

        self.main_thread = threading.Thread(target=self._main_thread, name="MainThread")

        self.input_thread = None
        self.output_thread = None
        self.cmd_thread = None

        signal.signal(signal.SIGINT, self.signal_handler)

    def _apply_loaded_config(self, cfg: dict, initial: bool = False):
        self.config = cfg or {}
        self.mid = (self.config.get("Middleware_ID", "mw1") or "mw1").encode("utf-8")

        self.upstreams_config = self.config.get("Upstreams", []) or []
        self.downstream_config = self.config.get("Downstream", {}) or {}
        self.downstream_addr = self.downstream_config.get("addr")
        self.down_mode = self.downstream_config.get("mode", "bind")

        if not self.downstream_addr:
            logger.error("'addr' for 'Downstream' is missing in config.")
            if initial:
                sys.exit(1)
            raise ValueError("Downstream.addr missing")

    def signal_handler(self, signum, frame):
        logger.info("Ctrl+C. closing...")
        self.stop_event.set()
        self.data_stop_event.set()
        self.cmd_stop_event.set()

    def start(self):
        logger.info(f"Starting middleware {self.mid.decode()} ...")
        self.main_thread.start()
        time.sleep(0.2)

        self._start_data_threads()
        self._start_cmd_thread()

        logger.info("All threads started.")

    def stop(self):
        self.stop_event.set()
        self.data_stop_event.set()
        self.cmd_stop_event.set()

        self._stop_data_threads()
        self._stop_cmd_thread()

        self.main_thread.join(timeout=2)
        try:
            self.context.term()
        except Exception:
            pass
        logger.info("Middleware stopped.")

    def _start_data_threads(self):
        self.data_stop_event.clear()
        self.input_thread = threading.Thread(target=self._input_thread, name="InputThread")
        self.output_thread = threading.Thread(target=self._output_thread, name="OutputThread")
        self.input_thread.start()
        self.output_thread.start()

    def _stop_data_threads(self):
        self.data_stop_event.set()
        for t in [self.input_thread, self.output_thread]:
            if t is not None:
                t.join(timeout=3)

    def _start_cmd_thread(self):
        self.cmd_stop_event.clear()
        self.cmd_thread = threading.Thread(target=self._cmd_thread, name="CmdThread")
        self.cmd_thread.start()

    def _stop_cmd_thread(self):
        self.cmd_stop_event.set()
        if self.cmd_thread is not None:
            self.cmd_thread.join(timeout=3)

    def _input_thread(self):
        logger.info("InputThread start.")
        poller = zmq.Poller()
        sockets = []

        input_pusher = self.context.socket(zmq.PAIR)
        input_pusher.setsockopt(zmq.LINGER, 0)
        input_pusher.connect(self.input_pusher_url)

        for up_info in self.upstreams_config:
            up_addr = up_info.get("addr")
            up_mode = up_info.get("mode", "connect")
            if not up_addr:
                logger.error("Upstream missing addr.")
                self.stop_event.set()
                self.data_stop_event.set()
                input_pusher.close()
                return

            sock = self.context.socket(zmq.ROUTER)
            sock.setsockopt(zmq.IDENTITY, self.mid)
            sock.setsockopt(zmq.LINGER, 0)

            if up_mode == "connect":
                sock.connect(up_addr)
                logger.info(f"[input] connect: {up_addr}")
            else:
                sock.bind(up_addr)
                logger.info(f"[input] bind: {up_addr}")

            poller.register(sock, zmq.POLLIN)
            sockets.append(sock)

        try:
            while (not self.stop_event.is_set()) and (not self.data_stop_event.is_set()):
                socks = dict(poller.poll(100))
                for s in socks:
                    frames = s.recv_multipart()
                    if len(frames) >= 2 and frames[1] == b"":
                        up_id = frames[0]
                        content = frames[2:]
                        input_pusher.send_multipart([up_id] + content)
        finally:
            input_pusher.close()
            for s in sockets:
                try:
                    poller.unregister(s)
                except Exception:
                    pass
                s.close()
            logger.info("InputThread stopped.")

    def _output_thread(self):
        logger.info("OutputThread start.")
        downstream_router = self.context.socket(zmq.ROUTER)
        downstream_router.setsockopt(zmq.IDENTITY, self.mid)
        downstream_router.setsockopt(zmq.LINGER, 0)

        if self.down_mode == "bind":
            downstream_router.bind(self.downstream_addr)
            logger.info(f"[output] bind: {self.downstream_addr}")
        else:
            downstream_router.connect(self.downstream_addr)
            logger.info(f"[output] connect: {self.downstream_addr}")

        main_to_output = self.context.socket(zmq.PAIR)
        main_to_output.setsockopt(zmq.LINGER, 0)
        main_to_output.connect(self.main_to_output_url)

        output_to_main = self.context.socket(zmq.PAIR)
        output_to_main.setsockopt(zmq.LINGER, 0)
        output_to_main.connect(self.output_to_main_url)

        poller = zmq.Poller()
        poller.register(downstream_router, zmq.POLLIN)
        poller.register(main_to_output, zmq.POLLIN)

        try:
            while (not self.stop_event.is_set()) and (not self.data_stop_event.is_set()):
                socks = dict(poller.poll(100))

                if downstream_router in socks:
                    frames = downstream_router.recv_multipart()
                    if len(frames) >= 2 and frames[1] == b"":
                        output_to_main.send_multipart([frames[0]] + frames[2:])

                if main_to_output in socks:
                    frames = main_to_output.recv_multipart()
                    if frames:
                        downstream_router.send_multipart(frames)
        finally:
            downstream_router.close()
            main_to_output.close()
            output_to_main.close()
            logger.info("OutputThread stopped.")

    def _cmd_thread(self):
        mid_str = self.mid.decode(errors="ignore")
        logger.info(f"CmdThread connect operator at {OP_ADDR} (id={mid_str})")

        sock = self.context.socket(zmq.DEALER)
        sock.setsockopt(zmq.IDENTITY, self.mid)  # command endpoint identity = mid
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(OP_ADDR)

        to_main = self.context.socket(zmq.PAIR)
        to_main.setsockopt(zmq.LINGER, 0)
        to_main.connect(self.cmd_to_main_url)

        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)

        registered = False
        last_hello = 0.0

        try:
            while (not self.stop_event.is_set()) and (not self.cmd_stop_event.is_set()):
                now = time.time()
                if (not registered) and (now - last_hello >= 1.0):
                    hello = {"type": "hello", "info": {"pid": os.getpid(), "state": self.state}, "ts": now}
                    sock.send(json.dumps(hello).encode("utf-8"))

                for _ in range(20):
                    try:
                        raw_out = self.cmd_out_queue.get_nowait()
                    except Empty:
                        break
                    sock.send(raw_out)

                socks = dict(poller.poll(100))
                if sock in socks:
                    raw = sock.recv()
                    try:
                        msg = json.loads(raw.decode("utf-8"))
                        if msg.get("type") == "hello_ack":
                            registered = True
                    except Exception:
                        pass
                    to_main.send(raw)
        finally:
            sock.close()
            to_main.close()
            logger.info("CmdThread stopped.")

    def _main_thread(self):
        logger.info("MainThread start.")

        # data inproc
        input_puller = self.context.socket(zmq.PAIR)
        input_puller.setsockopt(zmq.LINGER, 0)
        input_puller.bind(self.input_pusher_url)

        output_from_down = self.context.socket(zmq.PAIR)
        output_from_down.setsockopt(zmq.LINGER, 0)
        output_from_down.bind(self.output_to_main_url)

        to_output = self.context.socket(zmq.PAIR)
        to_output.setsockopt(zmq.LINGER, 0)
        to_output.bind(self.main_to_output_url)

        cmd_from_cmdthread = self.context.socket(zmq.PAIR)
        cmd_from_cmdthread.setsockopt(zmq.LINGER, 0)
        cmd_from_cmdthread.bind(self.cmd_to_main_url)

        poller = zmq.Poller()
        poller.register(input_puller, zmq.POLLIN)
        poller.register(output_from_down, zmq.POLLIN)
        poller.register(cmd_from_cmdthread, zmq.POLLIN)

        def enqueue_out(msg: dict):
            try:
                self.cmd_out_queue.put_nowait(json.dumps(msg).encode("utf-8"))
            except Exception:
                pass

        def send_reply(req_id, ok, message, extra=None):
            rep = {
                "type": "reply",
                "request_id": req_id,
                "ok": ok,
                "state": self.state,
                "message": message,
                "extra": extra or {},
                "ts": time.time()
            }
            enqueue_out(rep)

        def send_event(event_name: str, payload: dict):
            ev = {"type": "event", "event": event_name, "ts": time.time()}
            ev.update(payload or {})
            enqueue_out(ev)

        def backup_and_write_config(new_text: str) -> dict:
            os.makedirs(CONFIG_DIR, exist_ok=True)

            now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            backup_name = f"{now}.yaml.old"
            backup_path = os.path.join(CONFIG_DIR, backup_name)

            if os.path.exists(CONFIG_FILE):
                try:
                    os.replace(CONFIG_FILE, backup_path)
                except Exception:
                    with open(CONFIG_FILE, "r", encoding="utf-8") as fr:
                        old_text = fr.read()
                    with open(backup_path, "w", encoding="utf-8", newline="\n") as fw:
                        fw.write(old_text)
                    os.remove(CONFIG_FILE)

            tmp_path = CONFIG_FILE + ".tmp"
            with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
                f.write(new_text)
            os.replace(tmp_path, CONFIG_FILE)

            return {"backup": backup_name, "path": CONFIG_FILE}

        try:
            while not self.stop_event.is_set():
                socks = dict(poller.poll(100))

                if cmd_from_cmdthread in socks:
                    raw = cmd_from_cmdthread.recv()
                    try:
                        msg = json.loads(raw.decode("utf-8"))
                    except Exception:
                        continue

                    if msg.get("type") == "hello_ack":
                        continue
                    if msg.get("type") != "cmd":
                        continue

                    cmd = msg.get("cmd", "")
                    req_id = msg.get("request_id", "")
                    target = msg.get("target", "")
                    args = msg.get("args", {}) or {}

                    if target and target.encode("utf-8") != self.mid:
                        send_reply(req_id, False, f"wrong target: {target}")
                        continue

                    if cmd == "push_config":
                        try:
                            content = args.get("content", "")
                            if not isinstance(content, str):
                                raise ValueError("content must be string")
                            meta = backup_and_write_config(content)
                            extra = {
                                **meta,
                                "bytes": len(content.encode("utf-8")),
                                "src_name": args.get("src_name"),
                                "src_sha256": args.get("src_sha256"),
                            }
                            send_reply(req_id, True, "config replaced (backup created). apply via restart.",
                                       extra=extra)
                        except Exception as e:
                            send_reply(req_id, False, f"push_config failed: {e}")
                        continue

                    if cmd == "status":
                        extra = {
                            "data_enabled": self.data_enabled,
                            "downstreams": [k.decode() for k in self.downstream_clients.keys()],
                            "downstream_addr": self.downstream_addr,
                            "down_mode": self.down_mode,
                            "upstreams": self.upstreams_config,
                            "id": self.mid.decode(errors="ignore"),
                        }
                        send_reply(req_id, True, "ok", extra=extra)
                        continue

                    if cmd == "stop":
                        self.data_enabled = False
                        self.state = "STOPPED"
                        send_reply(req_id, True, "stopped (data forwarding paused)")
                        continue

                    if cmd == "start":
                        self.data_enabled = True
                        self.state = "RUNNING"
                        send_reply(req_id, True, "started")
                        continue

                    if cmd == "restart":
                        old_id = self.mid.decode(errors="ignore")
                        try:
                            self.data_enabled = False
                            self.state = "APPLYING"
                            self.data_stop_event.set()
                            self._stop_data_threads()

                            try:
                                poller.unregister(input_puller)
                                poller.unregister(output_from_down)
                            except Exception:
                                pass
                            input_puller.close()
                            output_from_down.close()
                            to_output.close()

                            cfg = load_config()
                            self._apply_loaded_config(cfg, initial=False)
                            new_id = self.mid.decode(errors="ignore")
                            self.downstream_clients.clear()

                            input_puller = self.context.socket(zmq.PAIR)
                            input_puller.setsockopt(zmq.LINGER, 0)
                            input_puller.bind(self.input_pusher_url)
                            output_from_down = self.context.socket(zmq.PAIR)
                            output_from_down.setsockopt(zmq.LINGER, 0)
                            output_from_down.bind(self.output_to_main_url)
                            to_output = self.context.socket(zmq.PAIR)
                            to_output.setsockopt(zmq.LINGER, 0)
                            to_output.bind(self.main_to_output_url)

                            poller.register(input_puller, zmq.POLLIN)
                            poller.register(output_from_down, zmq.POLLIN)

                            logger.info("Internal data sockets rebuilt successfully.")

                            self._start_data_threads()

                            id_changed = (new_id != old_id)
                            if id_changed:
                                send_event("id_change", {"old_id": old_id, "new_id": new_id, "pid": os.getpid()})

                            self.data_enabled = True
                            self.state = "RUNNING"

                            send_reply(req_id, True, "restart applied (data sockets rebuilt)", extra={
                                "old_id": old_id,
                                "new_id": new_id,
                                "id_changed": id_changed,
                                "downstream_addr": self.downstream_addr,
                                "down_mode": self.down_mode,
                                "upstreams": self.upstreams_config,
                            })

                            if id_changed:
                                self.cmd_stop_event.set()
                                self._stop_cmd_thread()
                                self._start_cmd_thread()

                        except Exception as e:
                            self.state = "ERROR"
                            logger.error(f"Restart failed: {e}")
                            send_reply(req_id, False, f"restart/apply failed: {e}")
                        continue

                    send_reply(req_id, False, f"unknown cmd: {cmd}")

                # ---- DATA from upstream ----
                if input_puller in socks:
                    frames = input_puller.recv_multipart()
                    if not frames:
                        continue
                    up_id = frames[0]
                    content = frames[1:]
                    if not self.data_enabled:
                        continue
                    if self.downstream_clients:
                        for down_id in list(self.downstream_clients.keys()):
                            to_output.send_multipart([down_id, b"", up_id] + content)

                if output_from_down in socks:
                    frames = output_from_down.recv_multipart()
                    if not frames:
                        continue
                    down_id = frames[0]
                    content = frames[1:]
                    if len(content) > 0 and content[0] == b"HELLO":
                        if down_id not in self.downstream_clients:
                            self.downstream_clients[down_id] = time.time()
                            logger.info(f"Registered downstream: {down_id.decode()}")
                        to_output.send_multipart([down_id, b"", b"HELLO_ACK"])

        finally:
            try:
                input_puller.close()
                output_from_down.close()
                to_output.close()
                cmd_from_cmdthread.close()
            except Exception:
                pass
            logger.info("MainThread stopped.")


if __name__ == "__main__":
    mw = Middleware()
    mw.start()
    try:
        while not mw.stop_event.is_set():
            time.sleep(0.2)
    finally:
        mw.stop()