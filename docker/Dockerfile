FROM daocloud.io/quantaxis/qactpbee:latest

RUN cd /root \
    && git clone https://github.com/yutiansut/QACTPBeeBroker \
    && cd QACTPBeeBroker && cp ./docker/config.ini ./QACTPBeeBroker/ \
    && pip install -e . 

COPY wait_for_it.sh /root/

RUN chmod +x /root/wait_for_it.sh

CMD ["QACTPBEE","--userid", "133496"]