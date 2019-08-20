FROM daocloud.io/quantaxis/qactpbee:latest

RUN cd /root \
&& git clone https://github.com/yutiansut/QACTPBeeBroker \
&& cd QACTPBeeBroker && cp ./docker/config.ini ./QACTPBeeBroker/ \
&& pip install -e .

CMD ["QACTPBEE","--userid", "133496"]