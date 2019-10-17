from time import sleep

from ctpbee.interface.ctp.td_api import BeeTdApi


class TdApi(BeeTdApi):

    def __init__(self):
        super().__init__(event_engine=None)

    def on_event(self, type, data):
        print(type, data)


if __name__ == '__main__':
    api = TdApi()
    login_info = {
                     "userid": "089131",
                     "password": "350888",
                     "brokerid": "9999",
                     # 24小时
                     "md_address": "tcp://180.168.146.187:10131",
                     "td_address": "tcp://180.168.146.187:10130",
                     # # 移动
                     # "md_address": "tcp://218.202.237.33:10112",
                     # "td_address": "tcp://218.202.237.33:10102",
                     "product_info": "",
                     "appid": "simnow_client_test",
                     "auth_code": "0000000000000000",
                 }
    api.connect(login_info)

    # api.send_order()
    # api.cancel_order()
    while True:
        api.query_account()
        sleep(2)
        api.query_position()
        sleep(2)