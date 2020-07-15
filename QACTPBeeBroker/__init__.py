from datetime import date
import click
import pymongo

from ctpbee import CtpBee, CtpbeeApi, auth_time, dumps
from QACTPBeeBroker.setting import eventmq_ip, ip
from QAPUBSUB.producer import publisher_routing, publisher_topic

eventmq_ip = eval(eventmq_ip)
__version__ = '1.5'
__author__ = 'yutiansut'


class DataRecorder(CtpbeeApi):
    def __init__(self, name, app=None, model='ctpx'):
        super().__init__(name, app)
        self.model = model

        if self.model == 'ctpx':
            self.pub = publisher_routing(host=eventmq_ip, exchange='CTPX', routing_key='')
        else:
            self.pub = publisher_topic(host=eventmq_ip, exchange='CTPPRO', routing_key='', durable=True)

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


@click.command()
@click.option('--userid', default="133496")
@click.option('--password', default="QCHL1234")
@click.option('--brokerid', default="9999")
@click.option('--mdaddr', default="tcp://218.202.237.33:10112")
@click.option('--tdaddr', default="tcp://218.202.237.33:10102")
@click.option('--appid', default="simnow_client_test")
@click.option('--authcode', default="0000000000000000")
def go(userid, password, brokerid, mdaddr, tdaddr, appid, authcode):
    app = CtpBee("last", __name__)
    info = {
        "CONNECT_INFO": {
            "userid": userid,
            "password": password,
            "brokerid": brokerid,
            "md_address": mdaddr,
            "td_address": tdaddr,
            "appid": appid,
            "auth_code": authcode,
        },
        "TD_FUNC": True,
    }

    app.config.from_mapping(info)
    data_recorder = DataRecorder("data_recorder")
    # 或者直接  data_recorder = DataRecorder("data_recorder", app)
    app.add_extension(data_recorder)
    app.start()
    print('start engine')
    import time
    time.sleep(5)
    contracts = app.recorder.get_all_contracts()
    print(contracts)
    cur_date = str(date.today())
    contractdb = pymongo.MongoClient(host=eval(ip)).QAREALTIME.contract
    for item in contracts:
        cont = item.__dict__
        cont['exchange'] = cont['exchange'].value
        cont['product'] = cont['product'].value
        cont['date'] = cur_date
        print(cont)
        try:
            contractdb.update_one({'gateway_name': 'ctp', 'symbol': cont['symbol']}, {
                '$set': cont}, upsert=True)
        except Exception as e:
            print(e)


@click.command()
@click.option('--userid', default="133496")
@click.option('--password', default="QCHL1234")
@click.option('--brokerid', default="9999")
@click.option('--mdaddr', default="tcp://218.202.237.33:10112")
@click.option('--tdaddr', default="tcp://218.202.237.33:10102")
@click.option('--appid', default="simnow_client_test")
@click.option('--authcode', default="0000000000000000")
@click.option('--realaddr', default="tcp://211.95.40.228:42213")
def gopro(userid, password, brokerid, mdaddr, tdaddr, appid, authcode, realaddr):
    app = CtpBee("last", __name__)
    info = {
        "CONNECT_INFO": {
            "userid": userid,
            "password": password,
            "brokerid": brokerid,
            "md_address": mdaddr,
            "td_address": tdaddr,
            "appid": appid,
            "auth_code": authcode,
        },
        "TD_FUNC": True,
        "MD_FUNC": False
    }
    another = {
        "CONNECT_INFO": {
            "userid": "",  # 期货账户名
            "password": "",  # 登录密码
            "brokerid": "8899",  # 期货公司id
            "md_address": realaddr,  # 行情地址
            "td_address": "",  # 交易地址
            "appid": "",  # 产品名
            "auth_code": "",  # 认证码
            "product_info": ""  # 产品信息
        },
        "INTERFACE": "ctp",  # 登录期货生产环境接口
        "TD_FUNC": False
    }
    NEW_APP = CtpBee("NEW", __name__)
    NEW_APP.config.from_mapping(another)
    app.config.from_mapping(info)
    data_recorder = DataRecorder("data_recorder", model='pro')
    # 或者直接  data_recorder = DataRecorder("data_recorder", app)
    # app.add_extension(data_recorder)
    app.start()
    NEW_APP.add_extension(data_recorder)
    NEW_APP.start()
    print('start engine')
    import time
    time.sleep(3)
    contracts = app.recorder.get_all_contracts()
    cur_date = str(date.today())
    contractdb = pymongo.MongoClient(host=eval(ip)).QAREALTIME.contract
    for item in contracts:
        cont = item.__dict__
        cont['exchange'] = cont['exchange'].value
        cont['product'] = cont['product'].value
        cont['date'] = cur_date
        NEW_APP.action.subscribe(item.local_symbol)
        contractdb.update_one({'gateway_name': 'ctp', 'symbol': cont['symbol']}, {
            '$set': cont}, upsert=True)


if __name__ == '__main__':
    go()
