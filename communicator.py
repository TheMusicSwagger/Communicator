import threading, socket, binascii, time, netifaces
import configuration

try:
    import pymysql
except:
    pass

class Packet(object):
    is_ready = None
    # indicate if the packet is ready or not

    from_cuid = None
    # source of the packet

    to_cuid = None
    # destination of the packet

    fonction_id = None
    # int representing the fonction id of the packet

    data = None
    # bytes representing the data which have to be sent

    packed_data = None

    # bytes representing the actual packed raw data

    def __init__(self):
        self.is_ready = False

    def __str__(self):
        return "from_cuid=" + str(self.get_from_cuid()) + ", to_cuid=" + str(self.get_to_cuid()) + ", fct_id=" + str(
            self.get_fonction_id()) + ", data=" + self.get_data().decode()

    def create(self, from_cuid, to_cuid, fonction_id, data=b''):
        self.set_from_cuid(from_cuid)
        self.set_to_cuid(to_cuid)
        self.set_fonction_id(fonction_id)
        self.set_data(data)
        return self

    def reconstruct(self, raw_packet):
        configuration.log("Reading : " + raw_packet.decode())
        try:
            self.set_to_cuid(int.from_bytes(raw_packet[:1], "big"))
            self.set_from_cuid(int.from_bytes(raw_packet[1:2], "big"))
            self.set_fonction_id(int.from_bytes(raw_packet[2:3], "big"))
            datlen = int.from_bytes(raw_packet[3:4], "big")
            self.set_data(raw_packet[4:4 + datlen])
            found_crc = int.from_bytes(raw_packet[4 + datlen:], "big")
            correct_crc = self.calculate_crc(raw_packet[:4 + datlen])
            if correct_crc != found_crc:
                configuration.warn("CRC Error : " + str(found_crc) + " != " + str(correct_crc) + " !")
            else:
                configuration.log("CRC correct : " + str(found_crc))
        except:
            configuration.warn("Parse error...")
        return self

    def build(self):
        if self.from_cuid is None or self.to_cuid is None or self.fonction_id is None or self.data is None:
            return self
        self.is_ready = True
        basic_packet_data = self.get_to_cuid().to_bytes(1, "big") + self.get_from_cuid().to_bytes(1,
                                                                                                  "big") + self.get_fonction_id().to_bytes(
            1, "big") + len(
            self.get_data()).to_bytes(1, "big") + self.get_data()
        self.set_packed_data(basic_packet_data + self.calculate_crc(basic_packet_data).to_bytes(1, "big"))
        return self

    def send(self, sock):
        """
        :param sock: a working UDP socket
        """
        if self.packed_data is None:
            self.build()
        if self.is_ready:
            try:
                broad_ip = None
                for iface in netifaces.interfaces():
                    try:
                        broad_ip = netifaces.ifaddresses(iface)[netifaces.AF_INET][0]["broadcast"]
                        # configuration.log("Found a correct interface : " + str(iface))
                        break
                    except:
                        pass
                        # configuration.log("Not a correct interface : " + str(iface))
                if broad_ip is None:
                    raise Exception("No interface available !")
                if self.to_cuid != configuration.CUID_SERVER or self.to_cuid == configuration.CUID_BROASCAST:
                    sock.sendto(self.get_packed_data(), (broad_ip, configuration.COMMUNICATION_PORT_DEVICE))
                if self.to_cuid == configuration.CUID_SERVER or self.to_cuid == configuration.CUID_BROASCAST:
                    sock.sendto(self.get_packed_data(), (broad_ip, configuration.COMMUNICATION_PORT_SERVER))

                return True
            except Exception as e:
                configuration.warn("Network error : " + "".join(e.args))
            configuration.log("Sending : " + self.get_packed_data().decode())
        else:
            configuration.warn("Packet is not ready !")
        return False

    def calculate_crc(self, data):
        return 0

    def get_from_cuid(self):
        return self.from_cuid

    def get_to_cuid(self):
        return self.to_cuid

    def get_fonction_id(self):
        return self.fonction_id

    def get_data(self):
        return self.data

    def get_packed_data(self):
        return self.packed_data

    def set_from_cuid(self, from_cuid):
        self.from_cuid = from_cuid
        return self

    def set_to_cuid(self, to_cuid):
        self.to_cuid = to_cuid
        return self

    def set_fonction_id(self, fonction_id):
        self.fonction_id = fonction_id
        return self

    def set_data(self, data):
        self.data = data
        return self

    def set_packed_data(self, packed_data):
        self.packed_data = packed_data
        return self

    def give_data_packet(self, from_cuid, data):
        try:
            packed_data = b''.join([val.to_bytes(configuration.DATA_VALUE_SIZE // 8, "big") for val in data])
            self.create(from_cuid, configuration.CUID_SERVER, configuration.FCT_GIVEDATA, packed_data)
        except OverflowError:
            pass
        return self

    def give_info_packet(self, from_cuid, to_cuid, data):
        self.create(from_cuid, to_cuid, configuration.FCT_INFO, data)
        return self

    def give_goodbye_packet(self, from_cuid):
        if from_cuid == configuration.CUID_SERVER:
            self.create(from_cuid, configuration.CUID_BROASCAST, configuration.FCT_GOODBYE)
        else:
            self.create(from_cuid, configuration.CUID_SERVER, configuration.FCT_GOODBYE)
        return self

    def give_spec_packet(self, from_cuid, nchans, name, desc):
        bname, bdesc = name.encode(), desc.encode()
        packed_data = nchans.to_bytes(1, "big") + len(bname).to_bytes(1, "big") + len(bdesc).to_bytes(1,
                                                                                                      "big") + bname + bdesc
        self.create(from_cuid, configuration.CUID_SERVER, configuration.FCT_MYSPEC, packed_data)
        return self


class Sender(threading.Thread):
    sock = None
    # socket used to send packets

    is_running = None
    # the status of the sender (allows to stop it)

    queue = None

    # packets waiting to be sent

    def __init__(self, sock):
        super(Sender, self).__init__()
        self.is_running = True
        self.sock = sock
        self.queue = []

        self.daemon = True
        # not waiting thread to stop before exiting program

        self.start()
        # starting the Thread
        configuration.log("Sender started !")

    def run(self):
        """
        Thread loop
        """
        try:
            while self.is_running:
                if len(self.queue) > 0:
                    current_packet = self.queue.pop(0)
                    self.is_running = current_packet.send(self.sock)
                    configuration.log("Sending : " + binascii.hexlify(current_packet.get_packed_data()).decode("ascii"))
                else:
                    time.sleep(0.01)
        finally:
            self.kill()

    def add_to_queue(self, packet):
        """
        Add a new packet to the queue.
        :param packet: the packet to send
        """
        self.queue.append(packet)
        return self

    def kill(self):
        """
        Kill the Thread.
        """
        self.is_running = False
        configuration.log("Sender killed !")
        return self


class Receiver(threading.Thread):
    sock = None
    # socket used to receive incomming packets

    is_running = None
    # the status of the receiver (allows to stop it)

    callback = None

    # callback of the 'Communicator' which will be called on receive
    # will give the received 'Packet' as argument

    def __init__(self, callback, sock):
        super(Receiver, self).__init__()
        self.is_running = True
        self.callback = callback
        self.sock = sock

        self.daemon = True
        # not waiting thread to stop before exiting program

        self.start()
        # starting the Thread
        configuration.log("Receiver started !")

    def run(self):
        """
        Thread loop
        """
        try:
            while self.is_running:
                raw_data, address = self.sock.recvfrom(configuration.MAX_PACKET_SIZE)
                packet = Packet().reconstruct(raw_data)
                configuration.log("Received : " + str(packet) + " : " + binascii.hexlify(raw_data).decode("ascii"))
                self.callback(packet)
        finally:
            self.kill()

    def kill(self):
        """
        Kill the Thread.
        """
        self.is_running = False
        configuration.log("Receiver killed !")
        return self


class Communicator(object):
    sock = None
    # socket udp to communicate data

    sender = None
    # thread to send data

    receiver = None
    # thread to receive data

    address = None
    # tuple ip/port

    database = None
    # sql database for config

    is_server = None
    # is this a server

    global_uid = None
    # guid value

    communication_uid = None
    # cuid value

    ready = None

    data_callback=None
    # the function to be called when data is received 'data_callback(cuid,data)'

    def __init__(self, is_server, guid="", data_callback=None):
        """
        :param guid: the guid of the device
        """
        super(Communicator, self).__init__()

        # setting up
        self.data_callback=data_callback

        self.is_server = is_server
        self.global_uid = guid

        # setting network
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            port = configuration.COMMUNICATION_PORT_DEVICE
            if self.is_server:
                port = configuration.COMMUNICATION_PORT_SERVER
            self.sock.bind(('', port))
            self.address = self.sock.getsockname()
            configuration.log("Listening on" + str(self.address))
        except:
            configuration.warn("Socket setup error !")

        self.sender = Sender(self.sock)
        self.receiver = Receiver(lambda p: self.on_receive(p), self.sock)

        if self.is_server:
            self.communication_uid = configuration.CUID_SERVER
            # databases setup
            try:
                self.database = pymysql.connect(host=configuration.DB_IP, user=configuration.DB_USER, password=configuration.DB_PASS, db=configuration.DB_NAME,
                                                charset=configuration.DB_CHARSET)
            except pymysql.err.Error:
                configuration.warn("Database setup error !")

            # clean tables
            self.db_query("DELETE FROM " + configuration.TB_CONNECTIONS)
            self.db_query("DELETE FROM " + configuration.TB_SPECIFICATIONS)

            self.ready = True
        else:
            # connection setup if
            self.init_connection()
            self.ready = False
        """
        self.daemon = True
        # wait thread to stop before exiting program
        """

        while not self.is_ready(): continue

    def init_connection(self):
        packed_data = binascii.unhexlify(self.get_guid())
        self.send(configuration.CUID_SERVER, 0x01, packed_data)
        # to server : IAMNEW

    def db_query(self, query, args=()):
        cursor = self.database.cursor()
        cursor.execute(query, args)
        dat = cursor.fetchall()
        cursor.close()
        self.database.commit()
        return dat

    def on_receive(self, packet):
        configuration.log(packet)
        if packet.get_to_cuid() == self.get_cuid() or packet.get_to_cuid() == configuration.CUID_BROASCAST:
            if packet.get_fonction_id() == configuration.FCT_INFO:
                # self.exec_callback(4, [packet.get_from_cuid(), packet.get_data()])
                print("Info : " + packet.get_data().decode("ascii"))
            elif packet.get_fonction_id() == configuration.FCT_IAMNEW and self.is_server:
                # I'M NEW
                other_guid = binascii.hexlify(packet.get_data()).decode("ascii")
                configuration.log("I'M NEW : " + str(other_guid))
                # finding new CUID
                if self.database is not None:
                    self.db_query("DELETE FROM " + configuration.TB_CONNECTIONS + " WHERE GUID=%s", (other_guid))
                    dat = self.db_query("SELECT CUID FROM " + configuration.TB_CONNECTIONS)
                    configuration.log("Already used CUIDs : " + str(dat))
                    is_ok = True
                    possible_cuid = 0
                    for possible_cuid in configuration.CUID_LIST_USABLE:
                        is_ok = True
                        for p in dat:
                            if possible_cuid in p:
                                is_ok = False
                        if is_ok:
                            break
                    if is_ok:
                        configuration.log("Found : " + str(possible_cuid))
                        # if CUID found -> register and send it
                        found_cuid = possible_cuid
                        self.db_query("INSERT INTO " + configuration.TB_CONNECTIONS + " (GUID, CUID) VALUES (%s, %s)",
                                      (other_guid, found_cuid))
                        # registered
                        self.send(configuration.CUID_BROASCAST, configuration.FCT_YOURETHIS,
                                  binascii.unhexlify(other_guid) + found_cuid.to_bytes(1, "big"))
                        # sent
                    else:
                        configuration.warn("No usable CUID found !")
                else:
                    self.send(configuration.CUID_BROASCAST, configuration.FCT_YOURETHIS,
                              binascii.unhexlify(other_guid) + (2).to_bytes(1, "big"))
            elif packet.get_fonction_id() == configuration.FCT_YOURETHIS and not self.is_server:
                # YOU'RE THIS
                configuration.log("YOU'RE THIS " + binascii.hexlify(packet.get_data()[:configuration.SIZE_GUID]).decode() + " - " + binascii.hexlify(packet.get_data()[configuration.SIZE_GUID:])).decode()
                if binascii.hexlify(packet.get_data()[:configuration.SIZE_GUID]).decode() == self.get_guid():
                    my_new_cuid = int.from_bytes(packet.get_data()[configuration.SIZE_GUID:], "big")
                    configuration.log("YOU'RE THIS : " + str(my_new_cuid))
                    self.set_cuid(my_new_cuid)
                    # got new cuid
                    self.send(packet.get_from_cuid(), configuration.FCT_INFO, b'Hello !')
                self.ready = True
            elif packet.get_fonction_id() == configuration.FCT_GIVEDATA and self.is_server:
                # GIVE DATA
                configuration.log("Give DATA :" + str(packet))
                if self.db_query("SELECT inited FROM " + configuration.TB_CONNECTIONS + " WHERE cuid=" + str(
                        packet.get_from_cuid()))[0][0] == 0:
                    configuration.log("Device not initialized !")
                    return


                vals = []
                for i in range(self.db_query("SELECT numchan FROM " + configuration.TB_SPECIFICATIONS + " WHERE cuid=" + str(
                        packet.get_from_cuid()))[0][0]):
                    vals.append(int.from_bytes(
                        packet.get_data()[(i * configuration.DATA_VALUE_SIZE // 8):((i + 1) * configuration.DATA_VALUE_SIZE // 8)],
                        "big"))
                print(str(packet.get_from_cuid())+ " -> "+str(vals))
                if self.data_callback:
                    self.data_callback(packet.get_from_cuid(),vals)
            elif packet.get_fonction_id() == configuration.FCT_MYSPEC and self.is_server:
                # MY SPEC
                configuration.log("Give MYSPEC :" + str(packet))
                # | NCHAN | NLEN | DLEN | NAME   | DESC   |
                # | (8b)  | (8b) | (8b) | (NLENb)| (DLENb)|
                nchan = packet.get_data()[0]
                nlen = packet.get_data()[1]
                dlen = packet.get_data()[2]
                name = ""
                desc = ""
                for i in range(nlen):
                    name += chr(packet.get_data()[3 + i])
                for i in range(dlen):
                    desc += chr(packet.get_data()[3 + nlen + i])
                configuration.log(name + " - " + desc)
                self.db_query("UPDATE " + configuration.TB_CONNECTIONS + " SET inited=%s WHERE CUID=%s",
                              (1, packet.get_from_cuid()))
                self.db_query("INSERT INTO " + configuration.TB_SPECIFICATIONS + " (numchan,name,description,cuid) VALUE (%s,%s,%s,%s)",
                              (nchan, name, desc,packet.get_from_cuid()))
                configuration.log("DB Query finished !")
            elif packet.get_fonction_id() == configuration.FCT_GOODBYE:
                # GOODBYE
                configuration.log("Give GOODBYE :" + str(packet))
                if self.is_server:
                    self.db_query("DELETE FROM " + configuration.TB_CONNECTIONS + " WHERE CUID=%s", (packet.get_from_cuid()))
                    self.db_query("DELETE FROM " + configuration.TB_SPECIFICATIONS + " WHERE CUID=%s", (packet.get_from_cuid()))
                else:
                    # self.callbacks['stop']()
                    pass

    def send(self, dest, fid, data=b''):
        """
        :param dest: entier representant le cuid du destinataire
        :param fid: entier representant la fonction demandee
        :param data: bytes correspondants aux donnees formates pour la fonction (pas de verification avant envoi)
        """
        self.sender.add_to_queue(Packet().create(self.get_cuid(), dest, fid, data).build())

    def stop(self):
        """
        Stoppe le thread du server.
        """
        self.receiver.kill()
        self.give_goodbye()
        self.receiver.join(1)
        # timeout to avaid waiting for a packet that will never come (if network problem)
        self.sender.kill()
        self.sender.join()
        self.sock.close()

    def get_address(self):
        return self.address

    def get_ip(self):
        return self.address[0]

    def get_port(self):
        return self.address[1]

    def get_guid(self):
        """
        :return: 'self.global_uid'
        """
        return self.global_uid

    def get_cuid(self):
        """
        :return: 'self.communication_uid'
        """
        if self.communication_uid is None:
            return configuration.CUID_BROASCAST
        return self.communication_uid

    def set_cuid(self, cuid):
        self.communication_uid = cuid

    def give_data_packet(self, data):
        self.sender.add_to_queue(Packet().give_data_packet(self.get_cuid(), data))

    def give_my_spec(self, nchans, name, desc):
        self.sender.add_to_queue(Packet().give_spec_packet(self.get_cuid(), nchans, name, desc))

    def give_info(self, info):
        self.sender.add_to_queue(Packet().give_info_packet(self.get_cuid(), configuration.CUID_BROASCAST, info))

    def give_goodbye(self):
        self.sender.add_to_queue(Packet().give_goodbye_packet(self.get_cuid()))

    def is_ready(self):
        return self.ready


if __name__ == "__main__":
    a = None
    try:
        guid = ""
        file = open("guid")
        guid = file.read().replace("\n", "")
        file.close()
        a = Communicator(True,guid)
        while True: continue
    except KeyboardInterrupt:
        configuration.log("User exiting...")
    finally:
        a.stop()
