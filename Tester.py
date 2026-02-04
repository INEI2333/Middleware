import threading
import zmq
import time

context = zmq.Context()

def upstream_component(upstream_url):
    socketup = context.socket(zmq.ROUTER)
    socketup.setsockopt(zmq.IDENTITY, b'Upstream')
    socketup.connect(upstream_url)
    time.sleep(1)
    for i in range(1000):
        message = [b'input_backend', b"Request", str(time.time()).encode(), b"Message from upstream %d" % i, b'', b'', b'']
        socketup.send_multipart(message)
        print(f"Upstream sent: {message}")
        #time.sleep(0.01)
    socketup.close()


def downstream_component(downstream_url):
    socketdown = context.socket(zmq.ROUTER)
    socketdown.setsockopt(zmq.IDENTITY, b'Downstream')
    socketdown.connect(downstream_url)
    time.sleep(1)
    message = socketdown.recv_multipart()
    print(f"Downstream received: {message}")
    socketdown.close()

if __name__ == "__main__":
    upstream_thread = threading.Thread(target=upstream_component, args=("tcp://localhost:5521",))
    downstream_thread = threading.Thread(target=downstream_component, args=("tcp://localhost:5522",))

    upstream_thread.start()
    downstream_thread.start()

    upstream_thread.join()
    downstream_thread.join()
