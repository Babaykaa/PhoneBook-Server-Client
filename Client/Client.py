import socket
import sys


class Client:
    """Client side of PhoneBookServer. Have a list of functions for exchange data with server and receiving
    information from it."""

    def __init__(self, family=socket.AF_INET, host=socket.gethostname(), port: int = 1313, backlog: int = 10, bufSize: int = 2048, consoleParameters: dict = None) -> None:
        """Initialize client socket connection."""
        if consoleParameters:
            host = host if parameters.get('host', None) is None else parameters['host']
            port = port if parameters.get('port', None) is None else parameters['port']
            backlog = backlog if parameters.get('backlog', None) is None else parameters['backlog']
            bufSize = bufSize if parameters.get('bufsize', None) is None else parameters['bufsize']
        self.socket = socket.socket(family, socket.SOCK_STREAM)
        self.__host = host
        self.__port = port
        self.__backlog = backlog
        self.__bufSize = bufSize
        self.socket.connect((host, port))
        self.exchangeDataWithServer()

    def getRequestFromServer(self) -> bool:
        """Function for getting messages from server."""
        MSGLEN = int(self.socket.recv(8).decode().rstrip())
        received_bytes = 0
        serverMessage = []
        while received_bytes < MSGLEN:
            data = self.socket.recv(min(MSGLEN - received_bytes, self.__bufSize))
            if not data:
                break
            serverMessage.append(data.decode())
            received_bytes += len(data.decode())

        message = ''.join(serverMessage)
        print(message)
        return True if 'exit' in message.lower() else False

    def sendCommandToServer(self) -> None:
        """Function for sending command to server."""
        msg = input(f'{self.__host} >>>\t')
        MSGLEN = str(len(msg))
        self.socket.send((MSGLEN + ' ' * (8 - len(MSGLEN))).encode())
        self.socket.send(msg.encode())

    def exchangeDataWithServer(self) -> None:
        """Function for exchanging data with server. Divided by 2 parts: getting request from server and sending
        command to it."""
        try:
            self.getRequestFromServer()
            while True:
                self.sendCommandToServer()
                res = self.getRequestFromServer()
                if res:
                    self.closeClient()
                    break
        except ConnectionResetError:
            self.closeClient()
        except KeyboardInterrupt:
            self.closeClient()
            print('Closing client...')
            exit(0)

    def closeClient(self) -> None:
        """Close socket connection with server."""
        self.socket.close()


if __name__ == '__main__':
    kwargs = sys.argv[1:]
    parameters = dict()
    for kwarg in kwargs:
        option, value = kwarg.lower().lstrip('-').split('=')
        if option in ['host', 'port', 'backlog', 'bufsize']:
            if option == 'host':
                parameters[option] = value
            else:
                parameters[option] = int(value)
        else:
            raise NameError(f'Unknown parameter {option} in list of parameters {kwargs}.')
    client_obj = Client(consoleParameters=parameters)
