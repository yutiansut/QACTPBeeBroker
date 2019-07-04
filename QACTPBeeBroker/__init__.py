import multiprocessing
from string import digits
import json
from json import dumps
from datetime import time, datetime, date
from time import sleep

from ctpbee import ExtAbstract
from ctpbee import CtpBee
from ctpbee import subscribe

from QAPUBSUB.producer import publisher_routing
import pymongo


def auth_time(timed):
    from datetime import time
    DAY_START = time(9, 0)  # 日盘启动和停止时间
    DAY_END = time(15, 0)
    NIGHT_START = time(21, 0)  # 夜盘启动和停止时间
    NIGHT_END = time(2, 30)

    if timed <= DAY_END and timed >= DAY_START:
        return True
    if timed >= NIGHT_START:
        return True
    if timed <= NIGHT_END:
        return True
    return False


class DataRecorder(ExtAbstract):
    def __init__(self, name, app=None):
        super().__init__(name, app)
        self.tick_database_name = "tick"
        self.bar_base_name = "bar"
        self.shared_data = {}
        self.created = False
        self.recover = False
        self.move = []
        self.mimi = set()
        self.pub = publisher_routing(exchange='CTPX', routing_key='')

    def on_trade(self, trade):
        pass

    def on_contract(self, contract):
        pass

    def on_order(self, order):
        pass

    def on_tick(self, tick):
        """tick process function"""
        symbol = tick.symbol
        r = tick.__dict__
        try:
            r['exchange'] = str(tick.exchange.value)
            r['datetime'] = str(r['datetime'])
            x = json.dumps(r)
            self.pub.pub(x, routing_key=r['symbol'])
        except Exception as e:
            print(e)

    def on_bar(self, bar):
        """bar process function"""
        bar.exchange = bar.exchange.value
        interval = bar.interval

    def on_shared(self, shared):
        """process shared function"""
        # print(shared)


def go():
    app = CtpBee("last", __name__)
    info = {
        "CONNECT_INFO": {
            "userid": "133496",
            "password": "QCHL1234",
            "brokerid": "9999",
            "md_address": "tcp://218.202.237.33:10112",
            "td_address": "tcp://218.202.237.33:10102",
            "appid": "simnow_client_test",
            "auth_code": "0000000000000000",
        },
        "TD_FUNC": True,
    }

    app.config.from_mapping(info)
    data_recorder = DataRecorder("data_recorder", app)
    app.start()
    print('start engine')
    import time
    time.sleep(5)
    contracts = app.recorder.get_all_contracts()
    print(contracts)
    cur_date = str(date.today())
    contractdb = pymongo.MongoClient().QAREALTIME.contract
    for item in contracts:
        cont = item.__dict__
        cont['exchange'] = cont['exchange'].value
        cont['product'] = cont['product'].value
        cont['date'] = cur_date
        print(cont)
        contractdb.update_one({'gateway_name': 'ctp', 'symbol': cont['symbol']}, {
            '$set': cont}, upsert=True)

    for contract in app.recorder.get_all_contracts():
        print(contract.symbol)
        subscribe(contract.symbol)


if __name__ == '__main__':
    go()
