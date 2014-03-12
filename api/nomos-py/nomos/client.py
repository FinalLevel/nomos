import socket
import exceptions

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO

class Nomos(object):
    """
        Implementation of the Nomos protocol.
    """

    @classmethod
    def __init__(self, host='localhost', port=7007, socketTimeout=3):
        self._host = host
        self._port = port
        self._socketTimeout = socketTimeout
        self._conn = None


    def _connect(self):
        if self._conn:
            return self._conn

        try:
            if hasattr(socket, 'create_connection'):
                sock = socket.create_connection((self._host, self._port), self._socketTimeout)
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self._socketTimeout)
                sock.connect((self._host, self._port))
            self._conn = sock
            return sock
        except socket.error:
            return None

    def _send(self, msg):
        try:
            self._conn.sendall(msg)
        except socket.error:
            pass

    MAX_BUFFER_SIZE = 32768
    def _receiveData(self, length):
        try:
            buf = BytesIO()
            bytesLeft = length
            while bytesLeft > 0:
                read_len = min(bytesLeft, self.MAX_BUFFER_SIZE)
                buf.write(self._conn.recv(read_len))
                bytesLeft = length - len(buf.getvalue())
            buf.seek(0)
            return buf.read(length)
        finally:
            buf.close()


    def _receiveAnswer(self):
        try:
            # An answer is always 11 bytes OK00000000\n
            response = self._conn.recv(11)
            if response[0] == 'E': # error
                if response.find("ERR_CR") != -1:
                    self._conn.close();
                    self._conn = None
                return None
            else:
                return response

        except socket.error:
            pass


    def get(self, level, subLevel, key, lifetime=0):
        if not self._connect():
            return None

        cmd = "V01,G," + str(level) + "," + str(subLevel) + "," + str(key) + "," + str(lifetime) + "\n"
        self._send(cmd)
        response = self._receiveAnswer()
        if response:
            dataLength = int(response[2:-1], 16)
            return self._receiveData(dataLength)
        else:
            return None

    def put(self, level, subLevel, key, lifetime, data):
        if not self._connect():
            return None

        dataLength = len(data);
        cmd = "V01,P," + str(level) + "," + str(subLevel) + "," + str(key) + "," + str(lifetime) + "," + \
              str(dataLength) + "\n" + data
        self._send(cmd)
        return self._receiveAnswer()

    def touch(self, level, subLevel, key, lifetime):
        if not self._connect():
            return None

        cmd = "V01,T," + str(level) + "," + str(subLevel) + "," + str(key) + "," + str(lifetime) + "\n"
        self._send(cmd)
        return self._receiveAnswer()

    def remove(self, level, subLevel, key):
        if not self._connect():
            return None

        cmd = "V01,R," + str(level) + "," + str(subLevel) + "," + str(key) + "\n"
        self._send(cmd)
        return self._receiveAnswer()
