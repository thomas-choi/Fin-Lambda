import socket
from os import environ

DDSmap = {'33': 'timestamp', '4':'4', '146':'146', '37':'high', '133':'open','32':'low', '3':'last',
          '0':'ticker','73':'spread_code','22':'22','23':'currency', '21':'name','505':'ch-name',
          '20':'20', '75':'75', '24':'24', '5':'5', '31':'pclose', '30':'30', '127':'close',
          '137':'137','17':'volume', '38':'TradeAmount', '1':'bid', '16':'bidvol', '2':'ask', '19':'askvol', 
          '25':'message', '39':'err_code'}


_tcpClient = None

class TCPClient:
    def __init__(self, ip_address, port):
        self.ip_address = ip_address
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(f'Connect: {ip_address},{port}')
        self.socket.connect((self.ip_address, self.port))
        # self.socket.settimeout(1)

    def __del__(self):
        print('Destructor: shutdown socket{self.socket}')
        self.socket.shutdown(socket.SHUT_RDWR)
        print('            close socket{self.socket}')
        self.socket.close()

    def convertRecord(self, message, maps):
        lt = message.split('|')
        # print(lt)
        record = dict()
        record['header'] = lt[0]
        record['subject'] = lt[1]
        for i in range(2, len(lt)-1, 2):
            # print(lt[i], ',', lt[i+1])
            if lt[i] in maps:
                record[maps[lt[i]]] = lt[i+1]
        return record

    def send_command_b(self, command):
        # print('send_command:', command.encode())
        self.socket.sendall(command.encode())
        # print('recv...')
        data = self.socket.recv(1024)
        # print('recv: ', data)
        return data.decode(errors='ignore')
    
    def send_command(self, command):
        print('send_command:', command.encode())
        self.socket.sendall(command.encode())
        data = b''
        print('recv...')
        while True:
            try:
                chunk = self.socket.recv(1024)
                if not chunk:
                    break
                data += chunk
            except socket.timeout:
                break
        print('recv: ', data)
        return data.decode()
    
    def snapshot(self, ticker, mymap = DDSmap):
        cmd = f'open|{ticker}|{ticker}|mode|image|\n'
        reply = self.send_command_b(cmd)
        return self.convertRecord(reply, mymap)
    
def defaultTCPClient():
    global  _tcpClient

    if _tcpClient is None:
        defaultIP=environ.get("defaultIP")
        defaultPort=int(environ.get("defaultPort"))
        _tcpClient = TCPClient(defaultIP, defaultPort)
    return _tcpClient

if __name__ == '__main__':
    from dotenv import load_dotenv
    import dataUtil as DU
    import json

    load_dotenv("Prod_config/Stk_eodfetch.env")
    # StockList = ['AMD','BAC','C','CSCO','DIS','DKNG','KO','MSFT','MU','NVDA','OXY','PYPL','TFC','TSLA','UBER','USB','VZ','WFC','XOM']
    # StockList = ['IBM']
    list_N = ["stock_list", "etf_list", "crypto_list", "us-cn_stock_list"]
    defaultIP=environ.get("defaultIP")
    defaultPort=int(environ.get("defaultPort"))
    DDSServer = TCPClient(defaultIP, defaultPort)
    market=dict()
    for lt in list_N:
        symbol_list = DU.load_symbols(lt)
        for sy in symbol_list:   
            reply = DDSServer.snapshot(sy)
            market[sy] = reply
    del DDSServer
    jstr = json.dumps(market)
    with open("market.json", "w") as outfile:
        outfile.write(jstr)
