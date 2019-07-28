from datetime import date

import pymongo
from QAPUBSUB.producer import publisher_routing
from ctpbee import CtpBee
from ctpbee import CtpbeeApi
from ctpbee import auth_time
from ctpbee import dumps


class DataRecorder(CtpbeeApi):
    def __init__(self, name, app=None):
        super().__init__(name, app)
        self.pub = publisher_routing(exchange='CTPX', routing_key='')

    def on_trade(self, trade):
        pass

    def on_contract(self, contract):
        """ 订阅所有合约代码 """
        self.app.subscribe(contract.symbol)

    def on_order(self, order):
        pass

    def on_log(self, log):
        """ 处理log信息 """
        pass

    def on_tick(self, tick):
        """ 处理tick推送 """
        if not auth_time(tick.datetime.time()):
            """ 过滤非交易时间的tick """
            return
        try:
            x = dumps(tick)  #
            self.pub.pub(x, routing_key=tick.symbol)
        except Exception as e:
            print(e)

    def on_bar(self, bar):
        """ 处理bar推送 """
        pass

    def on_position(self, position) -> None:
        """ 处理持仓推送 """
        pass

    def on_account(self, account) -> None:
        """ 处理账户信息 """
        pass

    def on_shared(self, shared):
        """ 处理分时图数据 """
        pass


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
    data_recorder = DataRecorder("data_recorder")
    app.add_extension(data_recorder)  # 或者直接  data_recorder = DataRecorder("data_recorder", app)
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


if __name__ == '__main__':
    go()
