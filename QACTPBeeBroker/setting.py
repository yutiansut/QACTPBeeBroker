import configparser
import os


config = configparser.ConfigParser()
try:
    config.read_file(open('{}{}config.ini'.format(
        os.path.dirname(os.path.abspath(__file__)), os.sep)))
    uri = config.get('database', 'mongouri')
    ip = config.get('database', 'mongoip')
    eventmq_ip = config.get('host', 'eventmq')

except:
    uri = 'mongodb://localhost:27017'
    ip = '127.0.0.1'
    eventmq_ip = '127.0.0.1'