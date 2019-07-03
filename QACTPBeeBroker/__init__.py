import multiprocessing
from string import digits
import json
from json import dumps
from datetime import time, datetime
from time import sleep

from ctpbee import ExtAbstract
from ctpbee import CtpBee
from ctpbee import subscribe

from QAPUBSUB.producer import publisher_routing


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

    def on_trade(self, trade):
        pass

    def on_contract(self, contract):
        pass

    def on_order(self, order):
        pass

    def on_tick(self, tick):
        """tick process function"""
        symbol = tick.symbol
        # print(tick)
        # print(dir(tick))#
        # print(type(tick))
        # print(tick.__str__)
        # print(tick.__dict__)
        r =  tick.__dict__
        #print()
        #r = json.dumps(tick.__dict__)
        try:
            r['exchange'] = str(tick.exchange.value)
            r['datetime'] = str(r['datetime'])
            x = json.dumps(r)
            
            publisher_routing(exchange='CTPX', routing_key=r['symbol']).pub(x, routing_key=r['symbol'])
        except Exception as e:
            print(e)
        #print(r)

    def on_bar(self, bar):
        """bar process function"""
        bar.exchange = bar.exchange.value
        interval = bar.interval

    def on_shared(self, shared):
        """process shared function"""
        # print(shared)





def go():
    app = CtpBee("last", __name__)
    # info = {
    #     "CONNECT_INFO": {
    #         "userid": "88715993",
    #         "password": "yt888999",
    #         "brokerid": "3040",
    #         "md_address": "tcp://180.169.85.204:61205",
    #         "td_address": "tcp://180.169.85.204:61213",
    #         "appid": "client_AQZPC_2.0.0",
    #         "auth_code": "SE6KFVIQ540AIDU2",
    #     },
    #     "TD_FUNC": False,
    # }
    info = {
        "CONNECT_INFO": {
            "userid": "089131",
            "password": "350888",
            "brokerid": "9999",
            "md_address": "tcp://180.168.146.187:10011",
            "td_address": "tcp://180.168.146.187:10001",
            "appid": "simnow_client_test",
            "auth_code": "0000000000000000",
        },
        "TD_FUNC": True,
    }

    app.config.from_mapping(info)
    data_recorder = DataRecorder("data_recorder", app)
    app.start()
    import time
    time.sleep(2)
    print(app.recorder.get_all_contracts())

    for contract in app.recorder.get_all_contracts():
        print(contract.symbol)
        subscribe(contract.symbol)


if __name__ == '__main__':
    go()
