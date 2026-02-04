import zmq
import threading
import time
import logging
import yaml
import sys
import signal
import os
from datetime import datetime

log_dir = "log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logfile_path = os.path.join(log_dir, f"{timestamp}_Middleware.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(threadName)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(logfile_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("middleware")

CONFIG_FILE = "middleware_config.yaml"


def load_config():
    """Load configuration file"""
    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {CONFIG_FILE}")
        sys.exit(1)


class Middleware:
    def __init__(self):
        self.config = load_config()
        self.mid = self.config.get("Middleware_ID").encode('utf-8')
        self.upstreams_config = self.config.get("Upstreams", [])
        self.downstream_config = self.config.get("Downstream", {})

        self.downstream_addr = self.downstream_config.get("addr")
        self.down_mode = self.downstream_config.get("mode", "bind")

        if not self.downstream_addr:
            logger.error("addr' for 'Downstream' is missing from the configuration file.")
            sys.exit(1)

        self.context = zmq.Context()
        self.stop_event = threading.Event()
        self.downstream_clients = {}

        self.input_pusher_url = "inproc://input_pusher"
        self.output_to_main_url = "inproc://output_to_main"
        self.main_to_output_url = "inproc://main_to_output"

        self.input_thread = threading.Thread(target=self._input_thread, name="InputThread")
        self.output_thread = threading.Thread(target=self._output_thread, name="OutputThread")
        self.main_thread = threading.Thread(target=self._main_thread, name="MainThread")

        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):
        logger.info("Ctrl+C. closing...")
        self.stop_event.set()

    def start(self):
        logger.info(f"Starting middleware {self.mid.decode()} ...")
        self.main_thread.start()
        time.sleep(0.2)
        self.input_thread.start()
        self.output_thread.start()
        logger.info(f"All threads have been started.")

    def stop(self):
        self.stop_event.set()
        self.main_thread.join()
        self.input_thread.join()
        self.output_thread.join()
        logger.info(f"Middleware {self.mid.decode()} terminated。")
        self.context.term()

    def _input_thread(self):
        logger.info("InputThread start.")
        poller = zmq.Poller()
        sockets = []

        try:
            input_pusher = self.context.socket(zmq.PAIR)
            input_pusher.setsockopt(zmq.LINGER, 0)
            input_pusher.connect(self.input_pusher_url)
        except zmq.ZMQError as e:
            logger.error(f"InputThread failed to connect to {self.input_pusher_url} : {e}")
            self.stop_event.set()
            return

        for up_info in self.upstreams_config:
            up_id_str = up_info.get('peer_id')
            up_addr = up_info.get('addr')
            up_mode = up_info.get('mode', 'connect')

            if not up_id_str or not up_addr:
                logger.error("Incorrect format，Missing 'peer_id' or 'addr'。")
                self.stop_event.set()
                return

            up_id = up_id_str.encode('utf-8')
            try:
                sock = self.context.socket(zmq.ROUTER)
                sock.setsockopt(zmq.IDENTITY, self.mid)
                sock.setsockopt(zmq.LINGER, 0)

                if up_mode == 'connect':
                    sock.connect(up_addr)
                    logger.info(f"[input] connect: {up_addr} ({up_id.decode()})")
                elif up_mode == 'bind':
                    sock.bind(up_addr)
                    logger.info(f"[input] bind: {up_addr} ({up_id.decode()})")

                poller.register(sock, zmq.POLLIN)
                sockets.append(sock)
            except zmq.ZMQError as e:
                logger.error(f"InputThread failed to create/bind/connect upstream {up_id.decode()} : {e}")
                self.stop_event.set()
                return

        try:
            while not self.stop_event.is_set():
                socks = dict(poller.poll(100))
                for sock in socks:
                    if sock in sockets:
                        # RECV MSG: [sender_id, '', content...]
                        frames = sock.recv_multipart()
                        if len(frames) >= 2 and frames[1] == b'':
                            up_id = frames[0]
                            content = frames[2:]
                            # FORWARD MSG TO MainThread: [up_id, content...]
                            input_pusher.send_multipart([up_id] + content)
                            logger.info(f"Message received from upstream {up_id.decode()}, forwarded to MainThread...")
        except zmq.ZMQError as e:
            if not self.stop_event.is_set():
                logger.error(f"InputThread ZMQ ERROR: {e}")
        finally:
            input_pusher.close()
            for sock in sockets:
                poller.unregister(sock)
                sock.close()
            logger.info("InputThread stopped.")

    def _output_thread(self):
        logger.info("OutputThread start.")
        try:
            downstream_router = self.context.socket(zmq.ROUTER)
            downstream_router.setsockopt(zmq.IDENTITY, self.mid)
            downstream_router.setsockopt(zmq.LINGER, 0)
            if self.down_mode == 'bind':
                downstream_router.bind(self.downstream_addr)
                logger.info(f"[output] bind: {self.downstream_addr}")
            elif self.down_mode == 'connect':
                downstream_router.connect(self.downstream_addr)
                logger.info(f"[output] connect: {self.downstream_addr}")

            main_to_output_puller = self.context.socket(zmq.PAIR)
            main_to_output_puller.setsockopt(zmq.LINGER, 0)
            main_to_output_puller.connect(self.main_to_output_url)

            output_to_main_pusher = self.context.socket(zmq.PAIR)
            output_to_main_pusher.setsockopt(zmq.LINGER, 0)
            output_to_main_pusher.connect(self.output_to_main_url)
        except zmq.ZMQError as e:
            logger.error(f"OutputThread failed to create/bind/connect socket: {e}")
            self.stop_event.set()
            return

        poller = zmq.Poller()
        poller.register(downstream_router, zmq.POLLIN)
        poller.register(main_to_output_puller, zmq.POLLIN)

        try:
            while not self.stop_event.is_set():
                socks = dict(poller.poll(100))

                if downstream_router in socks:
                    frames = downstream_router.recv_multipart()
                    if len(frames) >= 2 and frames[1] == b'':
                        output_to_main_pusher.send_multipart([frames[0]] + frames[2:])
                        logger.info(f"Message received from downstream {frames[0].decode()}, forwarded to MainThread...")

                if main_to_output_puller in socks:
                    frames = main_to_output_puller.recv_multipart()
                    if frames:
                        down_id = frames[0]
                        downstream_router.send_multipart(frames)
                        logger.info(f"Forward message from Main thread to Downstream {down_id.decode()}: {frames[2:]}")
        except zmq.ZMQError as e:
            if not self.stop_event.is_set():
                logger.error(f"OutputThread ZMQ ERROR: {e}")
        finally:
            downstream_router.close()
            main_to_output_puller.close()
            output_to_main_pusher.close()
            logger.info("OutputThread stopped.")

    def _main_thread(self):
        """主线程，处理逻辑"""
        logger.info("MainThread start.")
        try:
            input_puller = self.context.socket(zmq.PAIR)
            input_puller.setsockopt(zmq.LINGER, 0)
            input_puller.bind(self.input_pusher_url)

            output_to_main_puller = self.context.socket(zmq.PAIR)
            output_to_main_puller.setsockopt(zmq.LINGER, 0)
            output_to_main_puller.bind(self.output_to_main_url)

            main_to_output_pusher = self.context.socket(zmq.PAIR)
            main_to_output_pusher.setsockopt(zmq.LINGER, 0)
            main_to_output_pusher.bind(self.main_to_output_url)
        except zmq.ZMQError as e:
            logger.error(f"MainThread failed connect to socket: {e}")
            self.stop_event.set()
            return

        poller = zmq.Poller()
        poller.register(input_puller, zmq.POLLIN)
        poller.register(output_to_main_puller, zmq.POLLIN)

        try:
            while not self.stop_event.is_set():
                socks = dict(poller.poll(100))

                if input_puller in socks:
                    frames = input_puller.recv_multipart()
                    if not frames: continue
                    up_id = frames[0]
                    content = frames[1:]
                    logger.info(f"Receiving message from upstream {up_id.decode()} : {content}")

                    if self.downstream_clients:
                        for down_id in self.downstream_clients:
                            message = [down_id, b'', up_id] + content
                            main_to_output_pusher.send_multipart(message)
                    else:
                        logger.warning("No downstream, message discarded.")

                if output_to_main_puller in socks:
                    frames = output_to_main_puller.recv_multipart()
                    if not frames: continue
                    down_id = frames[0]
                    content = frames[1:]
                    logger.info(f"Receiving message from downstream {down_id.decode()} : {content}")

                    if len(content) > 0 and content[0] == b'HELLO':
                        if down_id not in self.downstream_clients:
                            self.downstream_clients[down_id] = time.time()
                            logger.info(f"Registered Downstream: {down_id.decode()}")
                            main_to_output_pusher.send_multipart([down_id, b'', b'HELLO_ACK'])

        except zmq.ZMQError as e:
            if not self.stop_event.is_set():
                logger.error(f"MainThread ZMQ ERROR: {e}")
        finally:
            input_puller.close()
            output_to_main_puller.close()
            main_to_output_pusher.close()
            logger.info("MainThread stopped.")


if __name__ == "__main__":
    mw = Middleware()
    try:
        mw.start()
        while not mw.stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        mw.stop()