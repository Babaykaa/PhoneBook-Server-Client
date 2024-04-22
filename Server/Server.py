import socket
import sys
import threading
import sqlite3
import traceback
import pathlib


class Server:
    """Server side of PhoneBookServer. Have a list of functions for exchange data with client and make changes in
    DataBase."""

    def __init__(self, family=socket.AF_INET, host=socket.gethostname(), port: int = 1313, backlog: int =10, bufSize: int =2048, consoleParameters: dict = None) -> None:
        """Initialize server socket class."""
        if consoleParameters:
            host = host if parameters.get('host', None) is None else parameters['host']
            port = port if parameters.get('port', None) is None else parameters['port']
            backlog = backlog if parameters.get('backlog', None) is None else parameters['backlog']
            bufSize = bufSize if parameters.get('bufsize', None) is None else parameters['bufsize']
        self.__server_db_path = pathlib.Path('').absolute().as_posix()
        self._phone_db = sqlite3.connect(self.__server_db_path + '/PhoneBook.db')
        self._cursor = self._phone_db.cursor()
        self.isTableExist()
        self.socket = socket.socket(family, socket.SOCK_STREAM)
        self.__host = host
        self.__port = port
        self.__backlog = backlog
        self.__bufSize = bufSize
        self.__client_sockets = []
        self.socket.bind((self.__host, self.__port))
        self.socket.listen(self.__backlog)
        self.__ip = self.socket.getsockname()[0]
        print(f'Server started at Host - {self.__host} with IP - {self.__ip} and Port - {self.__port}.')
        print(f'Remote IP - {socket.gethostbyname(self.__host)}')
        self.receiveConnections()
        self._cursor.close()
        self._phone_db.close()

    def isTableExist(self) -> None:
        """Check if database table exist and create it when needed."""
        try:
            self._cursor.execute('SELECT * FROM book LIMIT 1;').fetchall()
        except sqlite3.OperationalError:
            self._cursor.execute('CREATE TABLE book (ID INTEGER PRIMARY KEY AUTOINCREMENT, CLIENT_IP text NOT NULL, '
                                 'FIRSTNAME text, LASTNAME text, PATRONYMIC text, PHONE INTEGER, NOTE text);')
            self._phone_db.commit()

    def loadWelcomeString(self) -> bytes:
        """Return welcome server string."""
        return (b'Welcome to PhoneBook server. Here you can add, remove, search abonent or watch record with following commands:\n'
                b'- add: add <FirstName> <LastName> <Patronymic> <Phone> <Note>\n'
                b'- remove: remove <AbonentID>\n'
                b'- search: search <SearchParameter=...> or search <SearchParameter1=...> <SearchParameter2=...> <SearchParameterN=...>\n'
                b'- show: show <AbonentID>\n'
                b'- exit: exit - for interrupt connection with server\n'
                b'Instead of any parameter in add you can input None or use definition FirstName=... Phone=... and input not all parameters.'
                b'P.S. For field note use \' symbols. For instance, fill note field as - \'job colleague\'.')

    def sendWelcomeMsgToClient(self, client_sock) -> None:
        """Sending welcome message to connected client."""
        toClient = self.loadWelcomeString()
        toClientSize = str(len(toClient))
        client_sock.send((toClientSize + ' ' * (8 - len(str(toClientSize)))).encode())
        client_sock.send(toClient)

    def recieveCommand(self, client_sock, client_address) -> str:
        """Receiving command from server and return it for next processing."""
        data = client_sock.recv(8).decode().rstrip()
        if data:
            MSGLEN = int(data)
            received_bytes = 0
            msg = []
            while received_bytes < MSGLEN:
                data = client_sock.recv(min(MSGLEN - received_bytes, self.__bufSize))
                if not data:
                    break
                msg.append(data.decode())
                received_bytes += len(data)

            command = ''.join(msg)
            print(f'Received command - {command} from client {client_address[0]}:{client_address[1]}')
            return command
        else:
            return 'exit'

    def receiveConnections(self) -> None:
        """Waiting for client connections and then send it to another thread."""
        self.socket.setblocking(False)
        try:
            while True:
                try:
                    client_sock, client_addr = self.socket.accept()
                    self.__client_sockets.append((client_sock, client_addr))
                    print(f'Client connected with IP - {client_addr[0]} and port - {client_addr[-1]}')
                    threading.Thread(target=self.exchangeDataWithClient, args=(client_sock, client_addr)).start()
                except BlockingIOError:
                    pass
        except KeyboardInterrupt:
            print('Server closing...')
            for sock, addr in self.__client_sockets:
                try:
                    self.sendMessageToClient(sock, addr, 'exit')
                    sock.close()
                except OSError:
                    pass
            self.closeServer()
            exit(0)

    def sendMessageToClient(self, client_sock, client_address, message) -> None:
        """Send prepared message from server to client."""
        toClientSize = str(len(message))
        print(f'Sent message - {message} to client - {client_address[0]}:{client_address[1]}')
        client_sock.send((toClientSize + ' ' * (8 - len(str(toClientSize)))).encode())
        client_sock.sendall(message.encode())

    def addToDb(self, comBody, db, cursor, client_adress) -> str:
        """Add new row to table."""
        phone_translate_dict = {ord('-'): '', ord('+'): '', ord('('): '', ord(')'): '', ord('_'): ''}
        parameters = ['FIRSTNAME', 'LASTNAME', 'PATRONYMIC', 'PHONE', 'NOTE']
        addToTable = {parameter: None for parameter in parameters}
        if len(comBody) == 5:
            isRightOrder = True
            for num, el in enumerate(comBody):
                if len(el.split('=')) == 1 and isRightOrder:
                    addToTable[parameters[num]] = el
                elif len(el.split('=')) == 1 and not isRightOrder:
                    return f'Error: Incorrect command structure. Use defined labels only after undefined.'
                else:
                    try:
                        colName, value = el.split('=')
                        addToTable[colName.upper()] = value
                        isRightOrder = False
                    except KeyError:
                        return 'Error: Invalid parameter/parameters name.'
            if addToTable['PHONE'] is not None:
                addToTable['PHONE'] = int(addToTable['PHONE'].translate(phone_translate_dict))
            try:
                cursor.execute(f"INSERT INTO book (CLIENT_IP, FIRSTNAME, LASTNAME, PATRONYMIC, PHONE, NOTE) "
                               f"VALUES ({repr(client_adress[0])}, {repr(addToTable['FIRSTNAME'])}, {repr(addToTable['LASTNAME'])}, "
                               f"{repr(addToTable['PATRONYMIC'])}, {repr(addToTable['PHONE'])}, {repr(addToTable['NOTE'])});").fetchall()
                db.commit()
                return f'Abonent successfully added.'
            except sqlite3.OperationalError as err:
                return f'Error: No such abonent ID. {traceback.format_exc()}'
        elif len(comBody) == 0:
            return 'Error: Empty parameters.'
        elif len(comBody) > 5:
            return 'Error: Empty parameters.'
        else:
            try:
                for el in comBody:
                    colName, value = el.split('=')
                    if colName.upper() in ['FIRSTNAME', 'LASTNAME', 'PATRONYMIC', 'PHONE', 'NOTE']:
                        addToTable[colName.upper()] = value
                    else:
                        return 'Error: Invalid parameter/parameters name.'
                for key in addToTable:
                    if addToTable[key] is None:
                        addToTable[key] = 'None'
                if addToTable['PHONE'] is not None:
                    addToTable['PHONE'] = int(addToTable['PHONE'].translate(phone_translate_dict))
                try:
                    cursor.execute(f"INSERT INTO book (CLIENT_IP, FIRSTNAME, LASTNAME, PATRONYMIC, PHONE, NOTE) "
                                   f"VALUES ({repr(client_adress[0])}, {repr(addToTable['FIRSTNAME'])}, {repr(addToTable['LASTNAME'])}, "
                                   f"{repr(addToTable['PATRONYMIC'])}, {repr(addToTable['PHONE'])}, {repr(addToTable['NOTE'])});").fetchall()
                    db.commit()
                    return f'Abonent successfully added.'
                except sqlite3.OperationalError as err:
                    return f'Error: No such abonent ID. {traceback.format_exc()}'
            except KeyError:
                return 'Error: Invalid parameter/parameters name.'
            except ValueError:
                return 'Error: Input all parameters in right order or use all defined labels.'

    def removeFromDb(self, comBody, db, cursor, client_adress) -> str:
        """Remove column with pointed ID."""
        try:
            cursor.execute(f'DELETE FROM book WHERE ID = {int(comBody[0])} AND CLIENT_IP = {repr(client_adress[0])};').fetchall()
            db.commit()
            return f'Successfully deleted abonent with ID - {int(comBody[0])}.'
        except sqlite3.OperationalError:
            return 'Error: No such abonent ID.'

    def searchInDb(self, comBody, cursor, client_adress) -> str:
        """Search column by inputted parameter or parameters."""
        searchBy = dict()
        phone_translate_dict = {ord('-'): '', ord('+'): '', ord('('): '', ord(')'): '', ord('_'): ''}
        for el in comBody:
            colName, value = el.split('=')
            if colName.upper() in ['FIRSTNAME', 'LASTNAME', 'PATRONYMIC', 'PHONE', 'NOTE']:
                searchBy[colName.upper()] = value
            else:
                return 'Error: Invalid parameter/parameters name.'
        if searchBy.get('PHONE', None) is not None:
            searchBy['PHONE'] = int(searchBy['PHONE'].translate(phone_translate_dict))
        searchString = ""
        for key in searchBy:
            searchString += f"{key} = {repr(searchBy[key])} AND "
        try:
            res = cursor.execute(f'SELECT ID, FIRSTNAME, LASTNAME, PATRONYMIC, PHONE, NOTE FROM book WHERE {searchString[:-1]} CLIENT_IP = {repr(client_adress[0])};').fetchall()
            resStr = 'ID\tFIRSTNAME\tLASTNAME\tPATRONYMIC\tPHONE\tNOTE\n'
            for row in res:
                resStr += '\t'.join(map(str, row)) + '\n'
            return resStr
        except sqlite3.OperationalError:
            return 'Error: No such abonent ID.'

    def showInDb(self, comBody, cursor, client_adress) -> str:
        """Return from database record with selected ID."""
        try:
            if comBody and int(comBody[0]):
                res = cursor.execute(f'SELECT ID, FIRSTNAME, LASTNAME, PATRONYMIC, PHONE, NOTE FROM book WHERE ID = {int(comBody[0])} AND CLIENT_IP = {repr(client_adress[0])};').fetchall()
                resStr = 'ID\tFIRSTNAME\tLASTNAME\tPATRONYMIC\tPHONE\tNOTE\n'
                for row in res:
                    resStr += '\t'.join(map(str, row)) + '\n'
                return resStr
            else:
                return 'Error: Please input abonent ID.'
        except sqlite3.OperationalError:
            return 'Error: No such abonent ID.'

    def showAllFromDb(self, cursor, client_adress) -> str:
        """Return all records from database."""
        try:
            res = cursor.execute(f'SELECT ID, FIRSTNAME, LASTNAME, PATRONYMIC, PHONE, NOTE FROM book WHERE CLIENT_IP = {repr(client_adress[0])};').fetchall()
            resStr = 'ID\tFIRSTNAME\tLASTNAME\tPATRONYMIC\tPHONE\tNOTE\n'
            for row in res:
                resStr += '\t'.join(map(str, row)) + '\n'
            return resStr
        except sqlite3.OperationalError:
            return 'Error: No data in table.'
        except IndexError:
            return 'Error: No data in table.'

    def preprocessCommand(self, command):
        """Function fpr analysis of command from client."""
        comList = command.split("'")
        if len(comList) > 1:
            for num, el in enumerate(comList):
                if 'note=' in el.lower():
                    note_num = num + 1
                    break
            else:
                note_num = -2
            note_text = comList[note_num]
            commandKey = ''
            if note_num == -2 or (note_num == len(comList) - 2 and comList[-1] == ''):
                splittedCommand = comList[0].split()
                comBody = splittedCommand[1:]
                commandKey = splittedCommand[0]
                if 'note=' in comBody[-1].lower():
                    comBody[-1] += note_text
                else:
                    comBody.append(note_text)
            else:
                comBody = []
                for el_num in range(len(comList)):
                    if el_num == 0:
                        splittedCommand = comList[el_num].split()
                        comBody.extend(splittedCommand[1:])
                        commandKey = splittedCommand[0]
                    elif el_num != note_num:
                        comBody.extend(comList[el_num].split())
                    else:
                        comBody[-1] += note_text
            comList = [commandKey] + comBody
        else:
            comList = command.split()
            comBody = comList[1:]
        return comList, comBody

    def processingCommand(self, command, client_adress, db, cursor) -> str:
        """Processing command from client and return result as string."""
        comList, comBody = self.preprocessCommand(command)

        if comList[0] == 'add':
            return self.addToDb(comBody, db, cursor, client_adress)
        elif comList[0] == 'remove':
            return self.removeFromDb(comBody, db, cursor, client_adress)
        elif comList[0] == 'search':
            return self.searchInDb(comBody, cursor, client_adress)
        elif comList[0] == 'show':
            return self.showInDb(comBody, cursor, client_adress)
        elif comList[0] == 'showall':
            return self.showAllFromDb(cursor, client_adress)
        else:
            return 'Unknown command. Try to input it again.'

    def exchangeDataWithClient(self, client_sock, client_address) -> None:
        """Function for exchanging data with client. Divided by 2 parts: getting request from client and sending
        answer to it."""
        db = sqlite3.connect(self.__server_db_path + '/PhoneBook.db')
        cursor = db.cursor()
        client_sock.setblocking(True)
        try:
            self.sendWelcomeMsgToClient(client_sock)
            while True:
                message = self.recieveCommand(client_sock, client_address)
                if 'exit' == message.lower():
                    self.sendMessageToClient(client_sock, client_address, message)
                    client_sock.close()
                    break
                message = self.processingCommand(message, client_address, db, cursor)
                self.sendMessageToClient(client_sock, client_address, message)
            try:
                self.__client_sockets.remove(client_sock)
                client_sock.close()
            except ValueError:
                pass
        except ConnectionResetError:
            self.__client_sockets.remove(client_sock)
            client_sock.close()
        except ConnectionAbortedError:
            pass

    def closeServer(self) -> None:
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
    server_obj = Server(consoleParameters=parameters)
