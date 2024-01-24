from typing import Optional, Tuple, Dict, List, TypedDict
from collections.abc import Callable
import paho.mqtt.client as mqtt
from enum import Enum
from os import path
import threading
import functools
import argparse
import inspect
import signal
import socket
import queue
import json
from time import sleep

# NOTE!!!! THIS SCRIPT IS INCOMPLETE!!!!


# ===== Base Types =====


def filter_kwargs(func: Callable) -> Callable:
    # Ingore all function that use *args or **kwargs
    if any(
        param.kind == inspect.Parameter.VAR_KEYWORD
        for param in inspect.signature(func).parameters.values()
    ):
        return func

    @functools.wraps(func)
    def _filter_func(*args, **kwargs) -> any:
        return func(*args, **{
            k: v for k, v in filter(
                lambda pair: (
                    lambda param: param is not None and param.kind in (inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
                )(inspect.signature(func).parameters.get(pair[0], None)),
                kwargs.items()
            )
        })

    return _filter_func


class DecEncMixin:
    def __init__(self, *args, **kwargs) -> None:
        # Make sure that encoding can't be passed in as None
        encoding = kwargs.get("encoding", None)
        self._encoding: str = "utf-8" if encoding is None else encoding
        super().__init__(*args, **kwargs)

    def _encode(self, msg: str) -> Optional[bytes]:
        try:
            return msg.encode(self._encoding)
        except Exception as e:
            print(f"WARN! Failed to encode '{msg}': {e}")
            return None

    def _decode(self, data: bytes) -> Optional[str]:
        try:
            return data.decode(self._encoding)
        except Exception as e:
            print(f"WARN! Failed to decode {data}: {e}")
            return None


class MQTTManager:
    def __init__(self, broker_addr: str, broker_port: int, topics: Optional[Dict[str, Callable[[mqtt.Client, mqtt.MQTTMessage], None]]] = None) -> None:
        self._addr = broker_addr
        self._port = broker_port
        self._topics = topics

        self._mqtt = mqtt.Client()
        self._mqtt.on_connect = self._connect
        self._mqtt.on_message = self._message

        self._ready_ev = threading.Event()

    def listen(self, background: bool, connect_timeout: int = 60) -> None:
        self._mqtt.connect(self._addr, self._port, connect_timeout)
        (self._mqtt.loop_start if background else self._mqtt.loop_forever)()

    def wait_for_connection(self, timeout: Optional[int] = None) -> bool:
        ret = self._ready_ev.wait(timeout=timeout)

        if ret:
            self._ready_ev.clear()

        return ret

    def stop(self) -> None:
        self._mqtt.disconnect()
        self._mqtt.loop_stop(force=True)

    def _connect(self, client: mqtt.Client, _, flags: int, rc: int) -> None:
        if rc != 0:
            print(f"Failed to connect to {self._addr}:{self._port} ({rc})")

        print(f"Connected to server {self._addr}:{self._port}...")

        if self._topics is None:
            return

        for topic in self._topics.keys():
            print(f"Subscribing to topic '{topic}' ...")
            client.subscribe(topic)

        self._ready_ev.set()

    def _message(self, client: mqtt.Client, _, msg: mqtt.MQTTMessage):
        handler: Optional[Callable[[mqtt.Client, mqtt.MQTTMessage], None]] = self._topics.get(msg.topic, None)
        
        if handler is None:
            print(f"WARN! Received message on topic '{msg.topic}' with no handler?!?!??!?!")
            return

        handler(client, msg)

# ===== Enums =====


class MQTTPingType(Enum):
    PING_GLOBAL    = 0
    PING_BRIDGE    = 1
    PING_DISPENSER = 2


class TargetDevice(Enum):
    BRIDGE    = "bridge"
    DISPENSER = "dispenser"


# Messages coming from the robot
class RobotMessage(Enum):
    IDLE = "ready"
    BRIDGE_AT_FINAL = "to_final_ready"
    BRIDGE_AT_DISPENSER = "to_dispenser_ready"
    DISPENSER_DONE = "dispenser_ready"


# Messages going to the robot
class RobotCommand(Enum):
    DISPENSER = "dispenser"
    BRIDGE_TO_DISPENSER = "to_dispenser"
    BRIDGE_TO_FINALIZER = "to_final"


class EventType(Enum):
    BRIDGE_REQUEST    = 0
    BRIDGE_UNLOCK     = 1
    DISPENSER_REQUEST = 2


class RunState(Enum):
    IDLE = 0
    MOVING_BRIDGE = 1
    RUNNING_DISPENSER = 2


class BridgePosition(Enum):
    FINALIZER = "to_builder"
    DISPENSER = "to_dispenser"


# ===== Typed Dicts =====


# Json Message Dicts

class GlobalPing(TypedDict):
    id: str
    type: str


# Program Internal Dicts


class ProgEvent(TypedDict):
    type: EventType
    who: str # ID of requester
    target: Optional[BridgePosition] # Only set if type == EventType.BRIDGE_REQUEST


# ===== Program Classes =====


class MQTTCobotManager(DecEncMixin, MQTTManager):
    @filter_kwargs
    def __init__(self, broker_addr: str, broker_port: int, bridge_id: str, dispenser_id: str, ev_queue: "Queue[ProgEvent]") -> None:
        super().__init__(broker_addr, broker_port, {
            "global/ping/request": lambda *args: self._on_ping(*args, MQTTPingType.PING_GLOBAL),
            "robot/bridge/ping": lambda *args: self._on_ping(*args, MQTTPingType.PING_BRIDGE),
            "robot/dispenser/ping": lambda *args: self._on_ping(*args, MQTTPingType.PING_DISPENSER),

            "global/ping/response": self._on_ping_resp,

            "robot/bridge/move": lambda _, m: self._safe_call(m.payload,
                lambda obj: self._put_event(EventType.BRIDGE_REQUEST, obj["requestee"], BridgePosition(obj["position"]))
            ), # _on_bridge_request
            "robot/bridge/unlock": lambda _, m: self._safe_call(m.payload,
                lambda obj: self._put_event(EventType.BRIDGE_UNLOCK, obj["id"])
            ), # _on_bridge_unlock

            "robot/dispenser/load": lambda _, m: self._safe_call(m.payload,
                lambda obj: self._put_event(EventType.DISPENSER_REQUEST, obj["id"])
            ) # _on_dispenser_request
        })

        self._ev_queue = ev_queue
        self._id_collect_ev = threading.Event()
        self._collected_ids: Optional[List[str]] = None

        self._id: Dict[TargetDevice, str] = {
            TargetDevice.BRIDGE: bridge_id,
            TargetDevice.DISPENSER: dispenser_id
        }

    def collect_device_ids(self) -> List[str]:
        self._collected_ids = []

        self._mqtt.publish("global/ping/request", None)
        while self._id_collect_ev.wait(timeout=1):
            self._id_collect_ev.clear()

        ids = self._collected_ids
        self._collected_ids = None
        return ids

    def _put_event(self, etype: EventType, who: str, target: Optional[BridgePosition] = None) -> None:
        if etype == EventType.BRIDGE_REQUEST:
            if target is None:
                raise ValueError("target must be given for EventType.BRIDGE_REQUEST")
        elif target is not None:
            raise ValueError("target given for non EventType.BRIDGE_REQUEST event")

        ev = { "type": etype, "who": who }

        if target is not None:
            ev["target"] = target

        self._ev_queue.put(ev)

    def _as_dict(self, data: bytes) -> Optional[dict]:
        msg = self._decode(data)

        if msg is None:
            return None

        try:
            return json.loads(msg)
        except json.decoder.JSONDecodeError as e:
            print(f"WARN! Failed to parse incoming message '{msg}' as JSON: {e}")
            return None

    def _safe_call(self, data: bytes, func: Callable, *args, **kwargs) -> any:
        if (msg := self._as_dict(data)) is None:
            return None

        func(msg, *args, **kwargs)

    def _encode_id_json(self, device: TargetDevice, **kwargs) -> bytes:
        # Expanding **kwargs here allows for custom valuues to be passed into the JSON object
        # eg. Calling self._encode_id_json(device, type="test") produces: b'{ "id": "device", type: "type" }'
        # While calling self._encode_id_json(device) produces: b'{ "id": "device" }'
        return self._encode(json.dumps({ "id": self._id[device], **kwargs }))

    def _on_ping(self, client: mqtt.Client, msg: mqtt.MQTTMessage, ping_type: MQTTPingType) -> None:
        if len(msg.payload) > 0:
            print(f"WARN! Non NULL ping request received on '{msg.topic}' ...")
            return

        if ping_type == MQTTPingType.PING_GLOBAL:
            print("Received global ping ...")
            for device in TargetDevice:
                if (data := self._encode_id_json(device, type=device.value)) is not None:
                    print(f"... {device.value}")
                    client.publish("global/ping/response", data)
        elif ping_type == MQTTPingType.PING_BRIDGE:
            print("Received bridge ping ...")
            if (data := self._encode_id_json(TargetDevice.BRIDGE)) is not None:
                client.publish("robot/bridge/ping", data)
        elif ping_type == MQTTPingType.PING_DISPENSER:
            print("Received dispenser ping request ...")
            if (data := self._encode_id_json(TargetDevice.DISPENSER)) is not None:
                client.publish("robot/dispenser/ping", data)

    def _on_ping_resp(self, client: mqtt.Client, msg: mqtt.MQTTMessage) -> None:
        if self._collected_ids is None:
            print("WARN! Received global ping response while not collecting ...")
            return

        resp: GlobalPing = self._as_dict(msg.payload)

        if resp is not None and resp["type"] == "gopigo":
            self._collected_ids.append(resp["id"])
            self._id_collect_ev.set()


class CobotServer(DecEncMixin):
    @filter_kwargs
    def __init__(self, iface_addr: str, server_port: int, ev_queue: "Queue[ProgEvent]", stop_ev: threading.Event) -> None:
        super().__init__()

        self._addr = iface_addr
        self._port = server_port
        self._ev_queue = ev_queue
        self._stop_ev = stop_ev

        self._pos_lock_queues: Dict[BridgePosition, List[str]] = { }
        self._dispenser_lock_queue: List[str] = []
        self._state = RunState.IDLE

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.settimeout(1)
        self._sock.bind((self._addr, self._port))

        self._handlers: Dict[RobotMessage, Callable[[socket.socket], None]] = {
            # RobotMessage.IDLE: self._on_idle, -> default is to call _process_event_queue
            RobotMessage.BRIDGE_AT_FINAL: self._on_bridge_final,
            RobotMessage.BRIDGE_AT_DISPENSER : self._on_bridge_dispenser,
            RobotMessage.DISPENSER_DONE : self._on_dispenser_done
        }

    def serve(self) -> None:
        self._sock.listen()
        print(f"Listening on {self._addr}:{self._port} ...")

        while not self._stop_ev.is_set():
            try:
                client, addr = self._sock.accept()
            except socket.timeout: # Allows for clean exit using Ctrl+C
                continue

            print(f"Client connected: {addr[0]}:{addr[1]} ...")

            client.settimeout(1)
            if not self._handle_client(client):
                print(f"Client {addr[0]}:{addr[1]} disconnected ...")
                break

    def _get_lock_queue(self, pos: BridgePosition) -> List[str]:
        if (lock_queue := self._pos_lock_queues.get(pos, None)) is None:
            lock_queue = { }
            self._pos_lock_queues[pos] = lock_queue

        return lock_queue

    def _pop_event(self) -> Optional[ProgEvent]:
        try:
            return self._ev_queue.get(block=False)
        except queue.Empty:
            return None

    def _process_event_queue(self, client: socket.socket) -> None:
        while (ev := self._pop_event()) is not None:
            if ev["type"] == EventType.BRIDGE_REQUEST:
                self._get_lock_queue(ev["target"]).append(ev["who"])
            elif ev["type"] == EventType.BRIDGE_UNLOCK:
                for lock_queue in self._pos_lock_queues.values():
                    try:
                        lock_queue.remove(ev["who"])
                    except ValueError:
                        pass
            elif ev["type"] == EventType.DISPENSER_REQUEST:
                self._dispenser_lock_queue.append(ev["who"])

        if self._state != RunState.IDLE:
            return

        # First try to process dispenser related requests
        if len(self._dispenser_lock_queue) > 0:
            self._state = RunState.RUNNING_DISPENSER
            socket.sendall(self._encode(RobotCommand.DISPENSER.value + "\0"))
            return

        # This loop quarantiies
        for pos in BridgePosition:
            lock_queue = self._get_lock_queue(pos)
            
            
        for position, lock_queue in self._pos_lock_queues.items():
            if len(lock_queue) > 0:
                self._state = RunState.MOVING_BRIDGE
                

    def _handle_client(self, client: socket.socket) -> bool:
        while not self._stop_ev.is_set():
            try:
                if (resp := client.recv(1024)) is None or len(resp) < 1:
                    return True # Client disconnected
            except socket.timeout: # Allows for clean exit using Ctrl+C
                self._process_event_queue()
                continue

            if (msg := self._decode(resp)) is None:
                continue

            try:
                robot_msg = RobotMessage(msg.rstrip("\n").rstrip("\r"))
            except ValueError:
                print(f"WARN! Unknown message '{msg.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")}' from robot ...")
                continue

            if (handler := self._handlers.get(robot_msg, None)) is not None:
                print(f"Command from robot '{robot_msg}' ...")
                handler(client)
            else:
                self._process_event_queue()

        return True

    def _on_bridge_final(self, client: socket.socket) -> None:
        pass

    def _on_bridge_dispenser(self, client: socket.socket) -> None:
        pass

    def _on_dispenser_done(self, client: socket.socket) -> None:
        pass

# ===== Main =====


stop_ev = threading.Event()


def signal_handler(signal, stack_frame):
    print("SIGINT! Please wait ...")
    stop_ev.set()


def main(**kwargs) -> int:
    ev_queue: "Queue[ProgEvent]" = queue.Queue()
    mq_man = MQTTCobotManager(**kwargs, ev_queue=ev_queue)
    server = CobotServer(**kwargs, ev_queue=ev_queue, stop_ev=stop_ev)

    signal.signal(signal.SIGINT, signal_handler)

    mq_man.listen(True)
    print("Waiting for MQTT connection ...")
    mq_man.wait_for_connection()

    print("Collecting device ids ...")
    print(mq_man.collect_device_ids())

    server.serve() # Blocks until SIGINT
    mq_man.stop()

    print("Goodbye!")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Bridge/Dispenser controller', fromfile_prefix_chars='@')
    parser.add_argument("-i", "--iface-addr", default="0.0.0.0", help="Interface address to bind the server to")
    parser.add_argument("-p", "--port", default=50000, type=int, choices=range(0, 65535), dest="server_port", help="The port the server listens on")
    parser.add_argument("-b", "--broker-addr", default="192.168.1.130", help="The MQTT broker server address to use")
    parser.add_argument("-r", "--broker-port", default=1883, type=int, choices=range(0, 65535), help="The MQTT broker server port to use")
    parser.add_argument("--bridge-id", default=TargetDevice.BRIDGE.value, help="The ID that referes the bridge component")
    parser.add_argument("--dispenser-id", default=TargetDevice.DISPENSER.value, help="The ID that referes the dispenser component")
    exit(main(**vars(parser.parse_args(["@./bridge.conf"]) if path.isfile("./bridge.conf") else parser.parse_args())))

