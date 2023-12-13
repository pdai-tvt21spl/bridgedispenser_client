from typing import Optional
import paho.mqtt.client as mqtt
import threading
import socket
import time
import json
import copy

HOST = '192.168.100.70'
PORT = 50000
pose = '(-0.15,-0.5,0.3,0,-3.14,0)'

class RobotStatus():
    locked = False
    bridgeposition = "to_dispenser"
    requestee_id = ""
    lock_id = ""
    messageQueue = []


status = RobotStatus()
robotarm_ready = False
taskdone = True



class MQTTCtx:
    def __init__(self, client: Optional[mqtt.Client] = None) -> None:
        self.id_list = []
        self._client = client
        self._collect_ev = threading.Event()

    def set_client(self, client: Optional[mqtt.Client]) -> None:
        self._client = client

    def add_id(self, id: str) -> None:
        if id not in self.id_list:
            self.id_list.append(id)
        self._collect_ev.set()

    def collect_ids(self) -> None:
        client.publish("global/ping/request", None)

        while self._collect_ev.wait(timeout=1):
            self._collect_ev.clear()

def publish_to_gopigo(gopigo_id, name):
    client.publish("robot/gopigo/" + gopigo_id + "ext_update", '{"from": "' + name + '"}')

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
	
    if(rc != 0): 
        print("Could not connect! \n")
	
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # subscribe to needed topics, robot/final/unload and robot/final/ping.
    client.subscribe("robot/bridge/move")
    client.subscribe("robot/bridge/unlock")
    client.subscribe("robot/dispenser/load")
    client.subscribe("global/ping/request")
    client.subscribe("global/ping/response")
    

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata: MQTTCtx, msg):
    #check for ping message:
    if msg.topic == "global/ping/request":
        if len(msg.payload) == 0:
            client.publish("global/ping/response", '{"type": "bridge", "id": "bridge"}')
    #check for devices available on the broker.
    #if a device is not found on the list, append it to a list.
    if msg.topic == "global/ping/response":
        jsonobj = json.loads(msg.payload.decode())
        if jsonobj['type'] == "gopigo":
            userdata.add_id(jsonobj['id'])
    #this is the response to a null message in the ping subtopic.
    print("message from Broker: " + msg.payload.decode() + "from topic" + msg.topic)
    if(msg.topic =="robot/bridge/ping"):
        if len(msg.payload) == 0:
            client.publish("robot/bridge/ping", '{"id": "bridge"}')
    #move message. When a message with a valid id is received, add it to a queue.
    if msg.topic == "robot/bridge/move":
        #we check if there is a valid message with id.
        jsonobj = json.loads(msg.payload.decode("utf-8"))
        #message is of format {"position": "", "requestee": ""}
        if jsonobj['position'] == 'to_builder':
            status.requestee_id = jsonobj['requestee']
            status.messageQueue.append(("to_final", status.requestee_id))          
        elif jsonobj['position'] == 'to_dispenser':
            status.requestee_id = jsonobj['requestee']
            status.messageQueue.append(("to_dispenser", status.requestee_id))

    if(msg.topic == "robot/bridge/unlock"):
        #we check if there is a valid message with id.
        #if so, unlock the robot.
        jsonobj = json.loads(msg.payload.decode("utf-8"))
        if jsonobj['id'] == status.lock_id:
            status.locked = False
            if status.locked:
                print("locked")
            else:
                print("unlocked")
    
    if(msg.topic == "robot/dispenser/load"):
        jsonobj =  json.loads(msg.payload.decode("utf-8"))
        status.requestee_id = jsonobj['id']
        if 'id' in jsonobj:
            status.messageQueue.append(("dispenser", status.requestee_id))

ctx = MQTTCtx()
client = mqtt.Client(userdata=ctx)
client.on_connect = on_connect
client.on_message = on_message
#client.connect("localhost", 1883, 60)
client.connect("192.168.1.130", 1883, 60)

ctx.set_client(client)
ctx.collect_ids()
print(ctx.id_list)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_start()

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()
    conn, address = s.accept()
    with conn:
        print('Connected by', address)
        while True:      
            print(status.lock_id)
            data = conn.recv(1024)
            command = data.decode()
            #bridge has moved to final. Publish to ext_update
            if "to_final_ready" in command:
                print("bridge is on the final. ext update to " + status.lock_id)
                status.locked = True
                status.bridgeposition = "to_final"
                client.publish("robot/gopigo/" + status.lock_id + "/ext_update", '{"from": "bridge"}')
            #bridge has moved to dispenser. publish to ext_update
            elif "to_dispenser_ready" in command:
                print("bridge is on the dispenser. ext update to" + status.lock_id)
                status.locked = True
                status.bridgeposition = "to_dispenser"
                client.publish("robot/gopigo/"+status.lock_id+"/ext_update", '{"from": "bridge"}')
            #dispenser is ready.
            elif "dispenser_ready" in command:
                print("dispenser is done. ext update to" + status.lock_id)
                client.publish("robot/gopigo/0/ext_update", '{"from": "dispenser"}')
                client.publish("robot/gopigo/1/ext_update", '{"from": "dispenser"}')
            
            if "ready" in command:
                if status.locked == False:
                    #print("ready to get command")
                    taskdone = True
            if taskdone:
                #when we are ready to process the next command, pick the first
                #message in the message queue and do the task.
                if len(status.messageQueue) != 0:
                    print(status.messageQueue)
                    commandToHandle = status.messageQueue.pop(0)
                    activeTask = ""
                    if(commandToHandle[0] == "to_final"):
                        if status.bridgeposition == "to_final":
                            status.locked = True
                            client.publish("robot/gopigo/" + commandToHandle[1] +"/ext_update", '{"from": "bridge"}')
                            status.lock_id = copy.copy(commandToHandle[1])
                            print(status.lock_id)
                        else:
                            status.lock_id = copy.copy(commandToHandle[1])
                            print(status.lock_id)
                            activeTask = (commandToHandle[0] + '\0').encode()
                    elif (commandToHandle[0] == "to_dispenser"):
                        if status.bridgeposition == "to_dispenser":
                            status.locked = True
                            client.publish("robot/gopigo/" + commandToHandle[1] + "/ext_update", '{"from": "bridge"}')
                            status.lock_id = copy.copy(commandToHandle[1])
                            print(status.lock_id)
                        else:
                            status.lock_id = copy.copy(commandToHandle[1])
                            activeTask = (commandToHandle[0] + '\0').encode()
                    elif commandToHandle[0] == "dispenser":
                        print(status.lock_id)
                        activeTask = (commandToHandle[0] + '\0').encode()
                    if activeTask != "":
                        conn.sendall(activeTask)
            taskdone = False
            time.sleep(0.2)

        conn.close()