from types import TracebackType
from typing import Any, ContextManager, Self
import logging
import uuid
import time
import json

import numpy as np
import zmq

from pyramid.model.model import BufferData
from pyramid.model.events import NumericEventList
from pyramid.model.signals import SignalChunk
from pyramid.neutral_zone.readers.readers import Reader


# OpenEphys ZMQ message formats -- where did these come from?
# Nice but incomplete/informal docs here:
#   https://open-ephys.github.io/gui-docs/User-Manual/Plugins/ZMQ-Interface.html
# Messy sample client code here:
#   https://github.com/open-ephys-plugins/zmq-interface/blob/main/Resources/python_client/plot_process_zmq.py
# Actual concrete, legible server source code here:
#   https://github.com/open-ephys-plugins/zmq-interface/blob/main/Source/ZmqInterface.cpp#L359


def format_heartbeat(
    uuid: str,
    application: str = "Pyramid",
    encoding: str = 'utf-8'
) -> bytes:
    heartbeat_info = {
        "application": application,
        "uuid": uuid,
        "type": "heartbeat"
    }
    heartbeat_bytes = json.dumps(heartbeat_info).encode(encoding=encoding)
    return heartbeat_bytes


def parse_heartbeat(
    message: bytes,
    encoding: str = 'utf-8'
) -> dict[str, str]:
    return json.loads(message.decode(encoding=encoding))


def format_continuous_data(
    data: np.ndarray,
    stream_name: str,
    channel_num: int,
    sample_num: int,
    sample_rate: float,
    message_num: int = 0,
    timestamp: int = 0,
    encoding: str = 'utf-8',
) -> list[bytes]:
    content_info = {
        "stream": stream_name,
        "channel_num": channel_num,
        "num_samples": data.size,
        "sample_num": sample_num,
        "sample_rate": sample_rate
    }

    header_info = {
        "message_num": message_num,
        "type": "data",
        "content": content_info,
        "data_size": data.size * data.itemsize,
        "timestamp": timestamp
    }

    envelope_bytes = "DATA".encode(encoding=encoding)
    header_bytes = json.dumps(header_info).encode(encoding=encoding)
    return [envelope_bytes, header_bytes, data.tobytes()]


def parse_continuous_data(
    parts: list[bytes],
    dtype=np.float32,
    encoding: str = 'utf-8',
) -> tuple[str, dict, np.ndarray]:
    envelope = parts[0].decode(encoding=encoding)
    header_info = json.loads(parts[1].decode(encoding=encoding))
    data = np.frombuffer(parts[2], dtype=dtype)
    return (envelope, header_info, data)


def ttl_data_to_bytes(
    event_line: int,
    event_state: int,
    ttl_word: int,
) -> bytes:
    return bytes([event_line, event_state]) + ttl_word.to_bytes(length=8)


def ttl_data_from_bytes(
    data: bytes
) -> tuple[int, int, int]:
    event_line = int(data[0])
    event_state = int(data[1])
    ttl_word = int.from_bytes(data[2:10])
    return (event_line, event_state, ttl_word)


def format_event(
    data: bytes,
    stream_name: str,
    source_node: int,
    type: int,
    sample_num: int,
    message_num: int = 0,
    timestamp: int = 0,
    encoding: str = 'utf-8',
) -> list[bytes]:
    content_info = {
        "stream": stream_name,
        "source_node": source_node,
        "type": type,
        "sample_num": sample_num
    }

    if data is not None:
        data_size = len(data)
    else:
        data_size = 0
    header_info = {
        "message_num": message_num,
        "type": "event",
        "content": content_info,
        "data_size": data_size,
        "timestamp": timestamp
    }

    envelope_bytes = "EVENT".encode(encoding=encoding)
    header_bytes = json.dumps(header_info).encode(encoding=encoding)
    if data is not None:
        return [envelope_bytes, header_bytes, data]
    else:
        return [envelope_bytes, header_bytes]


def parse_event(
    parts: list[bytes],
    encoding: str = 'utf-8'
) -> tuple[str, dict, bytes]:
    envelope = parts[0].decode(encoding=encoding)
    header_info = json.loads(parts[1].decode(encoding=encoding))
    if len(parts) > 2:
        return (envelope, header_info, parts[2])
    else:
        return (envelope, header_info, None)


def format_spike(
    waveform: np.ndarray,
    stream_name: str,
    source_node: int,
    electrode: str,
    sample_num: int,
    sorted_id: int,
    threshold: list[float],
    message_num: int = 0,
    timestamp: int = 0,
    encoding: str = 'utf-8',
) -> list[bytes]:
    if len(waveform.shape) == 2:
        num_channels = waveform.shape[0]
        num_samples = waveform.shape[1]
    else:
        num_channels = 1
        num_samples = waveform.size
    spike_info = {
        "stream": stream_name,
        "source_node": source_node,
        "electrode": electrode,
        "sample_num": sample_num,
        "num_channels": num_channels,
        "num_samples": num_samples,
        "sorted_id": sorted_id,
        "threshold": threshold
    }

    # For some reason, spike content is called "spike" instead of "content" (condinuous data and events are both "content").
    # For some reason, spike headers don't include a data_size.
    header_info = {
        "message_num": message_num,
        "type": "spike",
        "spike": spike_info,
        "timestamp": timestamp
    }

    # For some reason, spike envelope is "EVENT", which makes it useless -- why not "SPIKE" to make it distinct?
    envelope_bytes = "EVENT".encode(encoding=encoding)
    header_bytes = json.dumps(header_info).encode(encoding=encoding)
    return [envelope_bytes, header_bytes, waveform.tobytes()]


def parse_spike(
    parts: list[bytes],
    dtype=np.float32,
    encoding: str = 'utf-8',
) -> tuple[str, dict, np.ndarray]:
    envelope = parts[0].decode(encoding=encoding)
    header_info = json.loads(parts[1].decode(encoding=encoding))
    spike_info = header_info.get("spike", {})
    num_channels = spike_info.get("num_channels", 1)
    num_samples = spike_info.get("num_samples", -1)
    waveform = np.frombuffer(parts[2], dtype=dtype).reshape([num_channels, num_samples])
    return (envelope, header_info, waveform)


class OpenEphysZmqServer(ContextManager):
    """Mimic the server side the Open Ephys ZMQ plugin -- as a standin for the actual Open Ephys application.

    The Open Ephys ZMQ plugin docs are here:
      https://open-ephys.github.io/gui-docs/User-Manual/Plugins/ZMQ-Interface.html

    This class is really only used for Pyramid automated testing.
    It's so closely related to the Pyramid reader and client code that it's convenient to include it here.
    """

    def __init__(
        self,
        host: str,
        data_port: int,
        heartbeat_port: int = None,
        scheme: str = "tcp",
        timeout_ms: int = 10,
        encoding: str = 'utf-8'
    ) -> None:
        self.data_address = f"{scheme}://{host}:{data_port}"

        if heartbeat_port is None:
            heartbeat_port = data_port + 1
        self.heartbeat_address = f"{scheme}://{host}:{heartbeat_port}"

        self.timeout_ms = timeout_ms
        self.encoding = encoding

        self.message_number = None
        self.last_heartbeat = None
        self.heartbeat_count = None

        self.heartbeat_reply = "heartbeat received"
        self.heartbeat_reply_bytes = self.heartbeat_reply.encode(encoding)

        self.context = None
        self.data_socket = None
        self.heartbeat_socket = None
        self.heartbeat_poller = None

    def __enter__(self) -> Self:
        self.context = zmq.Context()

        self.data_socket = self.context.socket(zmq.PUB)
        self.data_socket.setsockopt(zmq.LINGER, self.timeout_ms)
        self.data_socket.bind(self.data_address)

        self.heartbeat_socket = self.context.socket(zmq.REP)
        self.heartbeat_socket.setsockopt(zmq.LINGER, self.timeout_ms)
        self.heartbeat_socket.bind(self.heartbeat_address)
        self.heartbeat_poller = zmq.Poller()
        self.heartbeat_poller.register(self.heartbeat_socket, zmq.POLLIN)

        self.message_number = 0
        self.last_heartbeat = None
        self.heartbeat_count = 0

        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        if self.context is not None:
            self.context.destroy()

        self.context = None
        self.data_socket = None
        self.heartbeat_socket = None
        self.heartbeat_poller = None

    def poll_heartbeat_and_reply(self, timeout_ms: int = None) -> bool:
        if timeout_ms is None:
            timeout_ms = self.timeout_ms

        ready = dict(self.heartbeat_poller.poll(timeout_ms))
        if self.heartbeat_socket in ready:
            bytes = self.heartbeat_socket.recv(zmq.NOBLOCK)
            if bytes:
                self.last_heartbeat = parse_heartbeat(bytes, self.encoding)
                self.heartbeat_count += 1
                self.heartbeat_socket.send(self.heartbeat_reply_bytes)
                return True

        return False

    def send_continuous_data(
        self,
        data: np.ndarray,
        stream_name: str,
        channel_num: int,
        sample_num: int,
        sample_rate: float,
    ) -> int:
        message_num = self.message_number
        self.message_number += 1
        timestamp = round(time.time() * 1000)
        parts = format_continuous_data(
            data,
            stream_name,
            channel_num,
            sample_num,
            sample_rate,
            message_num,
            timestamp,
            self.encoding
        )
        self.data_socket.send_multipart(parts)
        return message_num

    def send_ttl_event(
        self,
        event_line: int,
        event_state: int,
        ttl_word: int,
        stream_name: str,
        source_node: int,
        sample_num: int,
    ) -> int:
        message_num = self.message_number
        self.message_number += 1
        type = 3  # ttl event
        data = ttl_data_to_bytes(event_line, event_state, ttl_word)
        timestamp = round(time.time() * 1000)
        parts = format_event(
            data,
            stream_name,
            source_node,
            type,
            sample_num,
            message_num,
            timestamp,
            self.encoding
        )
        self.data_socket.send_multipart(parts)
        return message_num

    def send_spike(
        self,
        waveform: np.ndarray,
        stream_name: str,
        source_node: int,
        electrode: str,
        sample_num: int,
        sorted_id: int,
        threshold: list[float],
    ) -> int:
        message_num = self.message_number
        self.message_number += 1
        timestamp = round(time.time() * 1000)
        parts = format_spike(
            waveform,
            stream_name,
            source_node,
            electrode,
            sample_num,
            sorted_id,
            threshold,
            message_num,
            timestamp,
            self.encoding
        )
        self.data_socket.send_multipart(parts)
        return message_num


class OpenEphysZmqClient(ContextManager):
    """Connect and subscribe as a client to an Open Ephys app running the ZMQ plugin.

    The Open Ephys ZMQ plugin docs are here:
      https://open-ephys.github.io/gui-docs/User-Manual/Plugins/ZMQ-Interface.html
    """

    def __init__(
        self,
        host: str,
        data_port: int,
        heartbeat_port: int = None,
        scheme: str = "tcp",
        timeout_ms: int = 10,
        encoding: str = 'utf-8',
        client_uuid: str = None
    ) -> None:
        self.data_address = f"{scheme}://{host}:{data_port}"

        if heartbeat_port is None:
            self.heartbeat_address = None
        else:
            self.heartbeat_address = f"{scheme}://{host}:{heartbeat_port}"

        self.timeout_ms = timeout_ms
        self.encoding = encoding

        if client_uuid is None:
            client_uuid = str(uuid.uuid4())
        self.client_uuid = client_uuid

        self.heartbeat_bytes = format_heartbeat(client_uuid)
        self.heartbeat_send_count = None
        self.heartbeat_reply_count = None

        self.context = None
        self.data_socket = None
        self.heartbeat_socket = None
        self.data_poller = None
        self.heartbeat_poller = None

    def __enter__(self) -> Self:
        self.context = zmq.Context()

        # Initially the SUB socket filters out / ignores all messages.
        # Setting an empty filter pattern allows all messages through.
        self.data_socket = self.context.socket(zmq.SUB)
        self.data_socket.setsockopt(zmq.SUBSCRIBE, b'')
        self.data_socket.setsockopt(zmq.LINGER, self.timeout_ms)
        self.data_socket.connect(self.data_address)
        self.data_poller = zmq.Poller()
        self.data_poller.register(self.data_socket, zmq.POLLIN)

        # Using a separate poller for data vs heartbeat allows them to wait/timeout independently.
        # Otherwise eg a heartbeat message could make it look like data was available, and we'd never wait for data.
        if self.heartbeat_address is not None:
            self.heartbeat_socket = self.context.socket(zmq.REQ)
            self.heartbeat_socket.setsockopt(zmq.LINGER, self.timeout_ms)
            self.heartbeat_socket.connect(self.heartbeat_address)
            self.heartbeat_poller = zmq.Poller()
            self.heartbeat_poller.register(self.heartbeat_socket, zmq.POLLIN)

        self.heartbeat_send_count = 0
        self.heartbeat_reply_count = 0

        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        if self.context is not None:
            self.context.destroy()

        self.context = None
        self.data_socket = None
        self.data_poller = None
        self.heartbeat_socket = None
        self.heartbeat_poller = None

    def send_heartbeat(self) -> bool:
        if self.heartbeat_socket is None:
            return False

        if self.heartbeat_send_count > self.heartbeat_reply_count:
            # Zmq only allows one outstanding REQ message at a time.
            logging.warning(f"OpenEphysZmqClient heartbeats out of sync: sent {self.heartbeat_send_count} heartbeats but reveived {self.heartbeat_reply_count} replies.")
            return False

        self.heartbeat_socket.send(self.heartbeat_bytes)
        self.heartbeat_send_count += 1
        return True

    def poll_and_receive_heartbeat(self, timeout_ms: int = None) -> str:
        if self.heartbeat_socket is None:
            return None

        if timeout_ms is None:
            timeout_ms = self.timeout_ms

        ready = dict(self.heartbeat_poller.poll(timeout_ms))
        if self.heartbeat_socket in ready:
            heartbeat_reply_bytes = self.heartbeat_socket.recv(zmq.NOBLOCK)
            if heartbeat_reply_bytes:
                self.heartbeat_reply_count += 1
                return heartbeat_reply_bytes.decode(self.encoding)
        return None

    def poll_and_receive_data(self, timeout_ms: int = None) -> dict[str, Any]:
        if timeout_ms is None:
            timeout_ms = self.timeout_ms

        results = {}
        ready = dict(self.data_poller.poll(timeout_ms))
        if self.data_socket in ready:
            parts = self.data_socket.recv_multipart(zmq.NOBLOCK)
            if parts:
                header_info = json.loads(parts[1].decode(self.encoding))

                data_type = header_info["type"]
                if data_type == "data":
                    (envelope, header_info, data) = parse_continuous_data(parts, encoding=self.encoding)
                    results.update(header_info)
                    results["envelope"] = envelope
                    results["data"] = data

                elif data_type == "event":
                    (envelope, header_info, data) = parse_event(parts, encoding=self.encoding)
                    if header_info.get("content", {}).get("type", None) == 3:  # ttl event
                        (event_line, event_state, ttl_word) = ttl_data_from_bytes(data)
                        results.update(header_info)
                        results["envelope"] = envelope
                        results["event_line"] = event_line
                        results["event_state"] = event_state
                        results["ttl_word"] = ttl_word

                elif data_type == "spike":
                    (envelope, header_info, waveform) = parse_spike(parts, encoding=self.encoding)
                    results.update(header_info)
                    results["envelope"] = envelope
                    results["waveform"] = waveform
                else:  # pragma: no cover
                    logging.warning(f"OpenEphysZmqClient ignoring unknown data type: {data_type}")

        return results


class OpenEphysZmqReader(Reader):
    """Subscribe to an Open Ephys app running the ZMQ plugin and read continuous data, events, and/or spikes.

    The Open Ephys ZMQ plugin docs are here:
      https://open-ephys.github.io/gui-docs/User-Manual/Plugins/ZMQ-Interface.html
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        data_port: int = 5556,
        heartbeat_port: int = 5557,
        event_sample_frequency: float = 1.0,
        continuous_data: dict[int, str] = None,
        events: str = None,
        spikes: str | dict[str, str] = None,
        heartbeat_interval: float = 1.0,
        client_uuid: str = None,
        scheme: str = "tcp",
        timeout_ms: int = 10,
        encoding: str = 'utf-8',
    ) -> None:
        """Create a new OpenEphysZmqReader.

        In the Open Ephys GUI, it looks like each instance of the ZMQ Interface is bound to a particular
        Open Ephys data "stream" and data port number.  From Pyramid, each OpenEphysZmqReader connects to that
        same data port, effectively selecting a data stream at the same time.  Each Pyramid reader can also
        configure which continuous data channels to keep, which spike electrodes or units to keep, whether
        to keep ttl events.

        Args:
            host:                   Open Ephys GUI IP address or host name to connect to
            data_port:              Open Ephys ZMQ Interface data port to connect to
            heartbeat_port:         Open Ephys ZMQ Interface heartbeat port to connect to (may be None to disable)
            event_sample_frequency: acquisition stream clock or sample rate, to convert sample numbers to timestamps
            continuous_data:        dictionary of {channel_num: buffer_name} to select which continuous data channels
                                    to keep and the buffer name for each one (default is None to not keep any)
            events:                 name of the buffer to receive ttl events (default is None to not keep ttl events)
            spikes:                 name of the buffer to receive all spike events, or a dictionary of
                                    {electrode_name: buffer_name} to select which spike electrodes to keep and the buffer
                                    name for each one (default is None to not keep any spikes)
            heartbeat_interval:     interval in seconds between hearbeat messages sent to the Open Ephys ZMQ Interface
            client_uuid:            unique id that this reader uses to identifiy itself ot the Open Ephys ZMQ Interface
            scheme:                 URL transport scheme to use when connecting to the Open Ephys ZMQ Interface
            timeout_ms:             how long to wait when polling for messages from the Open Ephys ZMQ Interface
            encoding:               binary encoding to use for string data
        """
        self.client = OpenEphysZmqClient(
            host,
            data_port,
            heartbeat_port,
            scheme,
            timeout_ms,
            encoding,
            client_uuid
        )

        self.event_sample_frequency = event_sample_frequency
        self.continuous_data = continuous_data
        self.events = events
        self.spikes = spikes

        self.heartbeat_interval = heartbeat_interval
        self.last_heartbeat_attempt = None

    def __enter__(self) -> Self:
        self.client.__enter__()
        self.last_heartbeat_attempt = 0
        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        return self.client.__exit__(__exc_type, __exc_value, __traceback)

    def get_initial(self) -> dict[str, BufferData]:
        initial = {}

        if self.continuous_data:
            for channel_num, name in self.continuous_data.items():
                # An incomplete placeholder to be amended when the first data arrive.
                initial[name] = SignalChunk(
                    sample_data=np.empty([0, 1], dtype='float32'),
                    sample_frequency=None,
                    first_sample_time=None,
                    channel_ids=[int(channel_num)]
                )

        if self.events:
            # [timestamp, ttl_word, event_line, event_state]
            initial[self.events] = NumericEventList(np.empty([0, 4], dtype=np.float64))

        if self.spikes:
            if isinstance(self.spikes, str):
                # [timestamp, sorted_id]
                initial[self.spikes] = NumericEventList(np.empty([0, 2], dtype=np.float64))
            elif isinstance(self.spikes, dict):
                for name in self.spikes.values():
                    # [timestamp, sorted_id]
                    initial[name] = NumericEventList(np.empty([0, 2], dtype=np.float64))

        return initial

    def read_next(self) -> dict[str, BufferData]:
        if self.client.heartbeat_socket is not None:
            now_time = time.time()
            heartbeat_elapsed = now_time - self.last_heartbeat_attempt
            if heartbeat_elapsed > self.heartbeat_interval:
                heartbeat_reply = self.client.poll_and_receive_heartbeat()
                if self.last_heartbeat_attempt > 0 and not heartbeat_reply:
                    logging.warning(f"Open Ephys ZMQ Interface at {self.client.data_address} has not replied to heartbeat for  at least {heartbeat_elapsed} seconds.")
                self.client.send_heartbeat()
                self.last_heartbeat_attempt = now_time

        client_results = self.client.poll_and_receive_data()
        if not client_results:
            return None

        results = {}
        data_type = client_results.get("type", None)
        if data_type == "data":
            if self.continuous_data:
                channel_num = client_results["content"]["channel_num"]
                name = self.continuous_data.get(channel_num, None)
                if name is not None:
                    sample_num = client_results["content"]["sample_num"]
                    sample_rate = client_results["content"]["sample_rate"]
                    sample_data = client_results["data"]
                    results[name] = SignalChunk(
                        sample_data=sample_data.reshape([-1, 1]),
                        sample_frequency=sample_rate,
                        first_sample_time=sample_num / sample_rate,
                        channel_ids=[int(channel_num)]
                    )

        elif data_type == "event":
            if self.events:
                # [timestamp, ttl_word, event_line, event_state]
                sample_num = client_results["content"]["sample_num"]
                timestamp = sample_num / self.event_sample_frequency
                ttl_word = client_results["ttl_word"]
                event_line = client_results["event_line"]
                event_state = client_results["event_state"]
                event_data = [timestamp, ttl_word, event_line, event_state]
                results[self.events] = NumericEventList(np.array([event_data], dtype=np.float64))

        elif data_type == "spike":
            if self.spikes:
                # Does Open Ephys give us anything like probe contact location or index or "channel" in the Plexon sense?
                # [timestamp, sorted_id]
                sample_num = client_results["spike"]["sample_num"]
                timestamp = sample_num / self.event_sample_frequency
                sorted_id = client_results["spike"]["sorted_id"]
                event_data = [timestamp, sorted_id]

                if isinstance(self.spikes, str):
                    results[self.spikes] = NumericEventList(np.array([event_data], dtype=np.float64))
                elif isinstance(self.spikes, dict):
                    electrode = client_results["spike"]["electrode"]
                    name = self.spikes.get(electrode, None)
                    if name is not None:
                        results[name] = NumericEventList(np.array([event_data], dtype=np.float64))

        # TODO: this is for debugging and should be removed to prevent log spam!
        if client_results and not results:  # pragma: no cover
            logging.warning(f"OpenEphysZmqReader ignoring unmapped data: {client_results}")

        return results
