"""This module provides access to the ACNET Control System
daemon (acnetd) via TCP protocol allowing Python scripts to
have ACNET connections to the control system.

Example: This simple example prints the ACNET ping reply
from CENTRA.

    import acnet
    import struct

    c = acnet.Connection()
    print c.request_single("ACNET@CENTRA", struct.pack("<h", 0), 2000)
"""

import array
import socket
import struct
import time
import threading
try:
    import queue
except ImportError:
    import Queue as queue


class Status(Exception):
    """An ACNET status type."""

    def __init__(self, val):
        """Creates a status value which is initialized with the supplied
        value.
        """
        super(Status, self).__init__()
        self.value = val

    @property
    def facility(self):
        """Returns the 'facility' code of a status value."""
        return self.value & 255

    @property
    def err_code(self):
        """Returns the 'error' code of a status value."""
        return self.value // 256

    def is_success(self):
        """Returns True if the status represents a success status."""
        return self.err_code == 0

    def is_fatal(self):
        """Returns True if the status represents a fatal status."""
        return self.err_code < 0

    def is_warning(self):
        """Returns True if the status represents a warning status."""
        return self.err_code > 0

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return self.value != other.value

    def __str__(self):
        return '[' + str(self.facility) + ' ' + str(self.err_code) + ']'

# This section associates common ACNET status codes with the
# acnet.Status class.


Status.ACNET_SUCCESS = Status(1 + 256 * 0)
Status.ACNET_PEND = Status(1 + 256 * 1)
Status.ACNET_ENDMULT = Status(1 + 256 * 2)
Status.ACNET_RETRY = Status(1 + 256 * -1)
Status.ACNET_NOLCLMEM = Status(1 + 256 * -2)
Status.ACNET_NOREMMEM = Status(1 + 256 * -3)
Status.ACNET_RPLYPACK = Status(1 + 256 * -4)
Status.ACNET_REQPACK = Status(1 + 256 * -5)
Status.ACNET_REQTMO = Status(1 + 256 * -6)
Status.ACNET_NOCON = Status(1 + 256 * -21)


class _ReplyGenerator:
    def __init__(self, input_queue):
        self.queue = input_queue
        self.blocking = True

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        """Implementation of next."""
        while True:
            try:
                status, addr, data = self.queue.get(self.blocking)

                if status.is_fatal():
                    raise status

                if status == Status.ACNET_PEND:
                    continue

                return (addr, data)
            except queue.Empty:
                return None


class Connection:
    """An object to manage connections to ACNET supporting
       the request/reply architecture for receiving data from
       ACNET services.  This module does not support python scripts
       to become services.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self):

        # Setup data structures

        self.name = 0
        self.cmd_lock = threading.Lock()
        self.data_lock = threading.Lock()
        self.ack_queue = queue.Queue(4)
        self.active_requests = dict()
        self.con_event = threading.Event()

        # Start the TCP socket read thread

        self.read_thread = threading.Thread(
            target=self._read_thread, name="AcnetThread")
        self.read_thread.setDaemon(True)
        self.read_thread.start()

    def __enter__(self):
        return self

    def __exit__(self, ext, exv, trb):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        return False

    # Convert rad50 value to a string
    @staticmethod
    def _rtoa(r50):
        result = array.array('c', "      ")
        chars = array.array('c', " ABCDEFGHIJKLMNOPQRSTUVWXYZ$.%0123456789")

        first_bit = r50 & 0xffff
        second_bit = (r50 >> 16) & 0xffff

        for index in range(0, 3):
            result[3 - index - 1] = chars[first_bit % 40]
            first_bit /= 40
            result[6 - index - 1] = chars[second_bit % 40]
            second_bit /= 40

        return result.tostring()

    # Convert a string to rad50 value
    @staticmethod
    def _ator(input_string):
        def char_to_index(char):
            # pylint: disable=too-many-return-statements
            if 'A' <= char <= 'Z':
                return ord(char) - ord('A') + 1
            if 'a' <= char <= 'z':
                return ord(char) - ord('a') + 1
            if '0' <= char <= '9':
                return ord(char) - ord('0') + 30
            if char == '$':
                return 27
            if char == '.':
                return 28
            if char == '%':
                return 29

            return 0

        first_bit = 0
        second_bit = 0
        s_len = len(input_string)
        for index in range(0, 6):
            char = input_string[index] if index < s_len else ' '

            if index < (6 / 2):
                first_bit *= 40
                first_bit += char_to_index(char)
            else:
                second_bit *= 40
                second_bit += char_to_index(char)

        return (second_bit << 16) | first_bit

    # Get the TCP host to use for the remote ACNET connection
    @staticmethod
    def _host():
        return ("firus-gate.fnal.gov", 6802)

    # Send commands and data to acnetd and wait for the ack

    def _send_command(self, cmd, data, ack):
        with self.cmd_lock:
            self.sock.sendall(cmd)
            self.sock.sendall(data)
            try:
                rpy = self.ack_queue.get(True, 2)
            except:
                raise Status.ACNET_NOCON

        return struct.unpack(ack, rpy)

    # Helper to fully receive count number of bytes from the socket

    def _recv_all(self, count):
        buf = b''

        while count:
            all_bytes = self.sock.recv(count)
            if not all_bytes:
                raise Status.ACNET_NOCON
            buf += all_bytes
            count -= len(all_bytes)

        return buf

    # Receive a reply from acnetd over the stream

    def _recv_reply(self):
        (mlen, mtype) = struct.unpack(">Ih", self._recv_all(6))
        return (mtype, self._recv_all(mlen - 2))

    # Thread for reading acks and data from the ACNET TCP socket

    def _read_thread(self):
        while True:
            try:
                if self.con_event.isSet():
                    (mtype, data) = self._recv_reply()

                    if mtype == 3:
                        (status, value, offset, reqid) = struct.unpack_from(
                            "<2xhBB8xh", data)

                        with self.data_lock:
                            unpacked_data = (Status(status), (value * 256) +
                                             offset, data[18:])

                            if reqid in self.active_requests:
                                self.active_requests[reqid].put(unpacked_data)
                            else:
                                new_queue = queue.Queue()
                                new_queue.put(unpacked_data)
                                self.active_requests[reqid] = new_queue

                    elif mtype == 2:
                        self.ack_queue.put(data)
                else:
                    # Setup the stream socket and connect.

                    self.sock = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM)
                    self.sock.setsockopt(
                        socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    self.sock.setblocking(True)
                    self.sock.connect(self._host())
                    self.sock.send(b"RAW\r\n\r\n")

                    # Send acnetd connect command and wait for reply

                    self.sock.sendall(struct.pack(
                        ">Ihhiiih", 18, 1, 1, 0, 0, 0, 0))
                    (_, data) = self._recv_reply()
                    (_, status, _, self.name) = struct.unpack(">hhBi", data)

                    # Setup no timeout and set the connection event

                    self.sock.settimeout(None)
                    self.con_event.set()

            except Exception:
                # Signal all the active generators to raise an exception

                for (_, new_queue) in self.active_requests.items():
                    new_queue.put((Status.ACNET_NOCON, 0, ""))

                # Cleanup and wait to attempt a reconnect

                self.active_requests = dict()
                self.sock.close()
                self.con_event.clear()
                time.sleep(2)

    # Translate node name to trunk/node value

    def get_node(self, name):
        """Translate a node name string to a trunk/node integer value
        """
        self.con_event.wait(1)
        cmd = struct.pack(">IhhiiI", 16, 1, 11, self.name, 0, self._ator(name))
        ack = ">hhH"
        (_, _, node) = self._send_command(cmd, b"", ack)
        return node

    # Helper method for sending requests

    def _send_request(self, path, data, flags, timeout):
        self.con_event.wait(1)
        task, node = path.rsplit('@')

        cmd = struct.pack(">IHHiiihhI", 24 + len(data), 1, 18, self.name, 0,
                          self._ator(task), self.get_node(node), flags, timeout)
        ack = ">hhh"
        (_, status, reqid) = self._send_command(cmd, data, ack)

        if status == 0:
            with self.data_lock:
                if reqid not in self.active_requests:
                    self.active_requests[reqid] = queue.Queue()

                return self.active_requests[reqid]

        raise status

    def request_multiple(self, path, data, timeout):
        """Send a request for multiple replies to the given path.
           The path parameter is in the form TASK@NODE where TASK is
           the task connection name of the service and NODE is the destination
           ACNET node name.  The timeout for the request is expressed in
           milliseconds.  The method return a generator class that supplies the
           multiple replies in the format (address, data).  The address is the
           trunk/node value of the service replier.
        """
        return _ReplyGenerator(self._send_request(path, data, 1, timeout))

    def request_single(self, path, data, timeout):
        """Send a request for a single reply to the given path.  This method takes
           The same parameters as request_multiple() and blocks until the reply is
           received or the timeout has occured.  The return format is also (address, data).
        """
        (status, addr, data) = self._send_request(
            path, data, 0, timeout).get(True)

        if status.is_fatal():
            raise status

        return (addr, data)


def main():
    """Main function that starts and takes command line arguments."""
    import binascii

    connection = Connection()

    while True:
        try:
            gen = connection.request_multiple(
                "TESTER@DCE46", struct.pack(">I", 16), 2000)

            (addr, data) = connection.request_single(
                "ACNET@DCE46", struct.pack(">h", 0), 2000)
            print(addr, binascii.hexlify(data))

            for (addr, data) in gen:
                print(addr, binascii.hexlify(data))
        except Status as status:
            print(status)
            time.sleep(2)


if __name__ == "__main__":
    main()
