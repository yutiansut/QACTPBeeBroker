from QAPUBSUB.consumer import subscriber_routing
from QACTPBeeBroker.setting import eventmq_ip
import click


@click.command()
@click.option('--code', default='rb1910')
def sub(code):
    x = subscriber_routing(host=eventmq_ip, exchange='CTPX', routing_key=code)

    import json

    def callback(a, b, c, data):
        print(json.loads(data))

    x.callback = callback
    x.start()


sub()
