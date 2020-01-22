import builtins
import logging
import threading
from urllib.parse import urlparse
import uuid

import tornado.websocket

import vaex
from vaex.utils import wrap_future_with_promise
from vaex.server import client
from .executor import Executor

logger = logging.getLogger("vaex.server.tornado")


class Client(client.Client):
    def __init__(self, hostname, port=5000, base_path="/", background=False, thread_mover=None, websocket=True, token=None,
                 token_trusted=None):
        self.hostname = hostname
        self.port = port
        self.base_path = base_path if base_path.endswith("/") else (base_path + "/")
        self.token = token
        self.token_trusted = token_trusted
        # if delay:
        event = threading.Event()
        self.thread_mover = thread_mover or (lambda fn, *args, **kwargs: fn(*args, **kwargs))
        logger.debug("thread mover: %r", self.thread_mover)

        # jobs maps from uid to tasks
        self.jobs = {}
        self.msg_reply_promises = {}
        self._msg_id_to_tasks = {}

        def ioloop_threaded():
            logger.debug("creating tornado io_loop")
            self.io_loop = tornado.ioloop.IOLoop().current()
            event.set()
            logger.debug("started tornado io_loop...")

            self.io_loop.start()
            self.io_loop.close()
            logger.debug("stopped tornado io_loop")

        io_loop = tornado.ioloop.IOLoop.current(instance=False)
        if True:  # io_loop:# is None:
            logger.debug("no current io loop, starting it in thread")
            thread = threading.Thread(target=ioloop_threaded)
            thread.setDaemon(True)
            thread.start()
            event.wait()
        else:
            logger.debug("using current io loop")
            self.io_loop = io_loop

        self.io_loop.make_current()

        self.executor = Executor(self)
        logger.debug("connect")
        self.connect()
        logger.debug("connected")
        self.update()

    @property
    def _url(self):
        protocol = "ws"
        return "%s://%s:%d%swebsocket" % (protocol, self.hostname, self.port, self.base_path)

    # def _connect(self):
    #     socket_future = tornado.websocket.websocket_connect(self._url)
    #     loop = asyncio.get_event_loop()
    #     self.socket = loop.run_until_complete(socket_future)


class ClientWebsocket(Client):
    def _send(self, msg, msg_id=None):
        if msg_id is None:
            msg_id = str(uuid.uuid4())
        self.msg_reply_promises[msg_id] = vaex.promise.Promise()
        auth = {'token': self.token, 'token-trusted': self.token_trusted}

        msg_encoding = vaex.encoding.Encoding()
        data = vaex.encoding.serialize({'msg_id': msg_id, 'msg': msg, 'auth': auth}, msg_encoding)

        def do():
            self.websocket.write_message(data, binary=True)
        self.io_loop.add_callback(do)  # make sure it gets executed from the right thread
        reply_msg, reply_encoding = self.msg_reply_promises[msg_id].get()
        return reply_msg['result'], reply_encoding

    def close(self):
        self.websocket.close()

    def _on_websocket_message(self, websocket_msg):
        if websocket_msg is None:
            return
        logger.debug("websocket msg: %r", websocket_msg)
        try:
            encoding = vaex.encoding.Encoding()
            websocket_msg = vaex.encoding.deserialize(websocket_msg, encoding)
            msg_id, msg = websocket_msg['msg_id'], websocket_msg['msg']
            if 'progress' in msg:
                fraction = msg['progress']
                for task in self._msg_id_to_tasks.get(msg_id, ()):
                    # TODO: handle cancel
                    self.thread_mover(task.signal_progress.emit, fraction)
            elif 'error' in msg:
                exception = RuntimeError("error at server: %r" % msg)
                self.msg_reply_promises[msg_id].reject(exception)
            elif 'exception' in msg:
                class_name = msg["exception"]["class"]
                msg = msg["exception"]["msg"]
                exception = getattr(builtins, class_name)(msg)
                self.msg_reply_promises[msg_id].reject(exception)
            else:
                self.msg_reply_promises[msg_id].fulfill((msg, encoding))
        except Exception as e:
            logger.exception("Exception interpreting msg reply: %r", websocket_msg)
            self.msg_reply_promises[msg_id].reject(e)

    def connect(self):
        url = self._url

        def connected(websocket):
            logger.debug("connected to websocket: %s" % url)
            self.websocket = websocket

        def failed(reason):
            logger.error("failed to connect to %s" % url)
        self.websocket_connected = vaex.promise.Promise()
        self.websocket_connected.then(connected, failed)

        def do():
            try:
                logger.debug("wrapping promise")
                logger.debug("connecting to: %s", url)
                connected = wrap_future_with_promise(tornado.websocket.websocket_connect(url,
                                                     on_message_callback=self._on_websocket_message))
                logger.debug("continue")
                self.websocket_connected.fulfill(connected)
            except:  # noqa
                logger.exception("error connecting")
        logger.debug("add callback")
        self.io_loop.add_callback(do)
        logger.debug("added callback: ")

        logger.debug("waiting for connection")
        self.websocket_connected.get()
        logger.debug("websocket connected")
        if self.websocket_connected.isRejected:
            raise self.websocket.reason


def connect(url, **kwargs):
    url = urlparse(url)
    if url.scheme == "ws":
        websocket = True
    else:
        websocket = False
    assert url.scheme in ["ws", "http"]
    port = url.port
    base_path = url.path
    hostname = url.hostname
    if websocket:
        return ClientWebsocket(hostname, base_path=base_path, port=port, **kwargs)
    elif url.scheme == "http":
        raise NotImplementedError("http not implemented")
        # return ClientHttp(hostname, base_path=base_path, port=port, **kwargs)
