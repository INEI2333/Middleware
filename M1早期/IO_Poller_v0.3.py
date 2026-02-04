import time
from threading import Thread, Event
import zmq
import logging

logging.basicConfig(filename='middleware_pressuretest.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

context = zmq.Context()

class Middleware:
    def __init__(self, compname, input_backend_url, output_backend_url, input_terminal_url, output_terminal_url,
                 channel):
        self.compname = compname
        self.input_backend_url = input_backend_url
        self.output_backend_url = output_backend_url
        self.input_terminal_url = input_terminal_url
        self.output_terminal_url = output_terminal_url
        self.channel = channel
        self.stop_event = Event()

        self.input_backend_socket = context.socket(zmq.ROUTER)
        self.input_backend_socket.setsockopt(zmq.IDENTITY, b'input_backend')
        self.input_backend_socket.bind(input_backend_url)
        logging.info(f"Input backend socket bound to {input_backend_url}")

        self.input_frontend_socket = context.socket(zmq.ROUTER)
        self.input_frontend_socket.setsockopt(zmq.IDENTITY, b'input_frontend')
        self.input_frontend_socket.bind(input_terminal_url)
        logging.info(f"Input frontend socket bound to {input_terminal_url}")

        self.input_terminal_socket = context.socket(zmq.ROUTER)
        self.input_terminal_socket.setsockopt(zmq.IDENTITY, b'input_terminal')
        self.input_terminal_socket.connect(input_terminal_url)
        logging.info(f"Input terminal socket connected to {input_terminal_url}")

        self.output_backend_socket = context.socket(zmq.ROUTER)
        self.output_backend_socket.setsockopt(zmq.IDENTITY, b'output_backend')
        self.output_backend_socket.bind(output_backend_url)
        logging.info(f"Output backend socket bound to {output_backend_url}")

        self.output_frontend_socket = context.socket(zmq.ROUTER)
        self.output_frontend_socket.setsockopt(zmq.IDENTITY, b'output_frontend')
        self.output_frontend_socket.bind(output_terminal_url)
        logging.info(f"Output frontend socket bound to {output_terminal_url}")

        self.output_terminal_socket = context.socket(zmq.ROUTER)
        self.output_terminal_socket.setsockopt(zmq.IDENTITY, b'output_terminal')
        self.output_terminal_socket.connect(output_terminal_url)
        logging.info(f"Output terminal socket connected to {output_terminal_url}")

        time.sleep(1)
        logging.info('Start handshake')

        # Thread Initialized
        self.input_thread = Thread(target=self.thread_input)
        self.output_thread = Thread(target=self.thread_output)
        self.main_thread = Thread(target=self.thread_main, daemon=True)
        logging.info("Checkpoint 1")

    def thread_input(self):
        input_poller = zmq.Poller()
        input_poller.register(self.input_backend_socket, zmq.POLLIN)
        input_poller.register(self.input_frontend_socket, zmq.POLLIN)
        input_poller.register(self.input_terminal_socket, zmq.POLLIN)

        while not self.stop_event.is_set():
            try:
                events = dict(input_poller.poll(3000))
                while events:
                    if self.input_backend_socket in events:
                        message = self.input_backend_socket.recv_multipart()
                        peer_id, signal, timestamp, msg, channel, sequence, reserved = message
                        logging.info(f"[Input Backend] Received message from {peer_id}: {message}")
                        self.input_frontend_socket.send_multipart(
                            [b'input_terminal', signal, timestamp, msg, channel, sequence, reserved])
                        logging.info(f"[Input Frontend] Forwarded message to Terminal: {message}")

                    events = dict(input_poller.poll(0))
            except Exception as e:
                logging.error(f"Error in thread_input: {e}")

    def thread_output(self):
        output_poller = zmq.Poller()
        output_poller.register(self.output_backend_socket, zmq.POLLIN)
        output_poller.register(self.output_frontend_socket, zmq.POLLIN)
        output_poller.register(self.output_terminal_socket, zmq.POLLIN)

        while not self.stop_event.is_set():
            try:
                events = dict(output_poller.poll(3000))
                while events:
                    if self.output_frontend_socket in events:
                        message = self.output_frontend_socket.recv_multipart()
                        logging.info(f"[Output Frontend] Received message from Terminal: {message}")
                        peer_id, signal, timestamp, msg, channel, sequence, reserved = message
                        peer_id=b'Downstream'
                        self.output_backend_socket.send_multipart(
                            [peer_id, signal, timestamp, msg, channel, sequence, reserved])
                        logging.info(f"[Output Backend] Forwarded message to {peer_id}: {message}")
                        logging.critical("[End] Finished a loop")

                    events = dict(output_poller.poll(0))
            except Exception as e:
                logging.error(f"Error in thread_output: {e}")

    def thread_main(self):
        main_poller = zmq.Poller()
        main_poller.register(self.input_terminal_socket, zmq.POLLIN)

        while not self.stop_event.is_set():
            try:
                events = dict(main_poller.poll(3000))
                while events:
                    if self.input_terminal_socket in events:
                        message = self.input_terminal_socket.recv_multipart()
                        peer_id, signal, timestamp, msg, channel, sequence, reserved = message
                        logging.info(f"[Main] Processing message: {message}")
                        processed_message = self.process_message([signal, timestamp, msg, channel, sequence, reserved])
                        self.output_terminal_socket.send_multipart([b'output_frontend'] + processed_message)
                        logging.info(f"[Main] Sent processed message to output: {processed_message}")
                    events = dict(main_poller.poll(0))
            except Exception as e:
                logging.error(f"Error in thread_main: {e}")

    def process_message(self, message):
        signal, timestamp, msg, channel, sequence, reserved = message
        msg = b'Msg to downstream'
        processed_message = [signal, timestamp, msg, channel, sequence, reserved]
        return processed_message

    def start(self):
        self.input_thread.start()
        self.output_thread.start()
        self.main_thread.start()

    def stop(self):
        self.stop_event.set()
        self.input_thread.join()
        self.output_thread.join()
        self.main_thread.join()

        self.input_backend_socket.close()
        self.input_frontend_socket.close()
        self.input_terminal_socket.close()
        self.output_backend_socket.close()
        self.output_frontend_socket.close()
        self.output_terminal_socket.close()
        context.term()


if __name__ == "__main__":
    middleware = Middleware(
        'comp1',
        'tcp://*:5521',  # input_backend_url
        'tcp://*:5522',  # output_backend_url
        'inproc://input-terminal',  # input_terminal_url
        'inproc://output-terminal',  # output_terminal_url
        b''
    )
    middleware.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping middleware...")
        middleware.stop()
        logging.info("Middleware stopped.")
