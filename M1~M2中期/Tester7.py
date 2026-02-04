import threading
import time
import zmq
import logging
from collections import defaultdict
import sys
import yaml
import signal
import os
from datetime import datetime

log_dir = "log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logfile_path = os.path.join(log_dir, f"{timestamp}_Tester.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(threadName)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(logfile_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("tester")


def load_config(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


try:
    config = load_config('middleware_config.yaml')
except FileNotFoundError:
    logger.error("configuration file 'middleware_config.yaml' not found. Please make sure the file exists.")
    sys.exit(1)

MW_ID = config.get('Middleware_ID', 'mw_instance_1').encode('utf-8')
upstreams_config = config.get('Upstreams', [])
downstream_config = config.get('Downstream', {})
downstream_addr = downstream_config.get('addr')
downstream_mw_mode = downstream_config.get('mode', 'bind')

UP_IDS = [up['peer_id'].encode('utf-8') for up in upstreams_config]

DOWN_IDS = [f"Down{i}".encode('utf-8') for i in range(1, 5)]
total_rounds = 5

global_ctx = zmq.Context.instance()

perf_lock = threading.Lock()
perf_data = defaultdict(list)
recv_history = defaultdict(set)
registered_downstreams = set()
all_downstreams_ready = threading.Event()


class TesterRunner:
    def __init__(self):
        self.threads = []
        self.stop_event = threading.Event()
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):
        self.stop_event.set()

    def run_downstream_tester(self, down_id):
        thread_name = f"Down-{down_id.decode()}"

        sock = global_ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.IDENTITY, down_id)
        sock.setsockopt(zmq.LINGER, 0)

        if downstream_mw_mode == 'bind':
            sock.connect(downstream_addr)
            logger.info(f"Downstream {down_id.decode()} connect {downstream_addr}")
        elif downstream_mw_mode == 'connect':
            sock.bind(downstream_addr)
            logger.info(f"Downstream {down_id.decode()} bind {downstream_addr}")

        time.sleep(0.5)

        registered = False
        for _ in range(3):
            sock.send_multipart([MW_ID, b'', b"HELLO"])
            logger.info(f"{down_id.decode()} send HELLO")

            try:
                if sock.poll(1000, zmq.POLLIN):
                    msg = sock.recv_multipart()
                    if len(msg) >= 3 and msg[0] == MW_ID and msg[1] == b"" and msg[2] == b"HELLO_ACK":
                        logger.info(f"Downstream {down_id.decode()} registered successfully")
                        registered = True
                        break
                    else:
                        logger.warning(f"Downstream {down_id.decode()} registration failed: {msg}")
            except zmq.ZMQError as e:
                logger.error(f"Downstream {down_id.decode()} ZMQ ERROR accrue when registered: {e}")

            if not registered:
                logger.warning(f"Downstream {down_id.decode()} registration timeout, retrying...")
                time.sleep(1)

        if not registered:
            logger.error(f"Downstream {down_id.decode()} final registration failed, closing...")
            self.stop_event.set()
            sock.close()
            return

        with perf_lock:
            registered_downstreams.add(down_id)
            if len(registered_downstreams) == len(DOWN_IDS):
                all_downstreams_ready.set()

        try:
            while not self.stop_event.is_set():
                if sock.poll(100, zmq.POLLIN):
                    msg = sock.recv_multipart()
                    if len(msg) < 7 or msg[0] != MW_ID or msg[1] != b'':
                        continue

                    up_id = msg[2]
                    seq = msg[5].decode().split('-')[1]
                    sent_timestamp = float(msg[6].decode())
                    latency = (time.time() - sent_timestamp) * 1000

                    with perf_lock:
                        perf_data[(up_id, down_id, seq)].append(latency)
                        recv_history[(up_id, down_id)].add(int(seq))

                    logger.info(f"Downstream {down_id.decode()} received: {msg} latency={latency:.2f}ms")
        except zmq.ZMQError as e:
            logger.error(f"{thread_name} ZMQ error: {e}")
        except Exception as e:
            logger.error(f"{thread_name} error: {e}", exc_info=True)
        finally:
            sock.close()
            logger.info(f"{thread_name} terminated.")

    def run_upstream_tester(self, up_id, up_addr, up_mode):
        """作为上游 ROUTER 发送消息"""
        thread_name = f"Up-{up_id.decode()}"

        all_downstreams_ready.wait(timeout=10)
        if not all_downstreams_ready.is_set():
            logger.error(f"Upstream {up_id.decode()} waited for downstream registration to time out, closing...")
            return
        logger.info(
            f"Upstream {up_id.decode()} received all downstream registration success signals, starting to send messages.")

        sock = global_ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.IDENTITY, up_id)
        sock.setsockopt(zmq.LINGER, 0)

        sock.bind(up_addr)
        logger.info(f"Upstream {up_id.decode()} bind {up_addr}")

        time.sleep(1)

        try:
            for i in range(total_rounds):
                if self.stop_event.is_set():
                    break
                message = [MW_ID, b'', b'REQUEST', up_id, f'DATA-{i}'.encode(), str(time.time()).encode()]
                sock.send_multipart(message)
                logger.info(f"Upstream {up_id.decode()} sending: {message}")
                time.sleep(1)
        except zmq.ZMQError as e:
            logger.error(f"{thread_name} ZMQ error: {e}")
        finally:
            sock.close()
            logger.info(f"{thread_name} terminated.")

    def run(self):
        logger.info("ZeroMQ Tester started.")
        logger.info("Waiting for middleware to fully start...")

        for down_id in DOWN_IDS:
            t = threading.Thread(target=self.run_downstream_tester, args=(down_id,), name=f"Down-{down_id.decode()}")
            t.start()
            self.threads.append(t)

        for up_info in upstreams_config:
            up_id = up_info.get('peer_id').encode('utf-8')
            up_addr = up_info.get('addr')
            up_mode = up_info.get('mode', 'connect')
            t = threading.Thread(target=self.run_upstream_tester, args=(up_id, up_addr, up_mode),
                                 name=f"Up-{up_id.decode()}")
            t.start()
            self.threads.append(t)

    def print_performance_report(self):
        logger.info("\n==== Performance Statistics Report ====")
        all_latencies = []

        for up_id in UP_IDS:
            for down_id in DOWN_IDS:
                path_key = f"{up_id.decode()}->{down_id.decode()}"

                logger.info(f" {path_key}:")
                for i in range(total_rounds):
                    latency_key = (up_id, down_id, str(i))
                    latencies = perf_data.get(latency_key, [])
                    if latencies:
                        avg_latency = sum(latencies) / len(latencies)
                        logger.info(
                            f"  Data {i}: Latency={avg_latency:.2f}ms Count={len(latencies)}")
                        all_latencies.extend(latencies)
                    else:
                        logger.warning(f"  Data {i}: [Lost]")

        logger.info("\n------ Summary ------")
        if all_latencies:
            logger.info(f" Overall average latency={sum(all_latencies) / len(all_latencies):.2f}ms")
            logger.info(f" Max={max(all_latencies):.2f}ms, Min={min(all_latencies):.2f}ms")
            logger.info(f" Total messages received={len(all_latencies)}")

        expected_total = len(UP_IDS) * len(DOWN_IDS) * total_rounds
        actual_received = len(all_latencies)
        lost_count = expected_total - actual_received
        logger.info(f" Total messages should be {expected_total}，actual received {actual_received}，lost {lost_count}")

        logger.info("\n[Packet Loss/Out-of-Order Detection]")
        for up_id in UP_IDS:
            for down_id in DOWN_IDS:
                path_key = f"{up_id.decode()}->{down_id.decode()}"

                expected_seq = set(range(total_rounds))
                received_seq = recv_history.get((up_id, down_id), set())

                missing = sorted(list(expected_seq - received_seq))

                logger.info(f" {path_key} Lost: {missing if missing else 'None'}")


if __name__ == '__main__':
    runner = TesterRunner()
    try:
        runner.run()
        while threading.active_count() > 1:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
    finally:
        runner.print_performance_report()