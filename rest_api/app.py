#!/usr/bin/python3
'''
This module is a Flask app that implements REST_API.
Use URLs written in the @app.route to create HTTP requests.

To run this file type into console:
>$ export FLASK_APP=app.py
>$ flask run
 
'''


import sys
from flask import Flask, jsonify, abort

sys.path.append('..')

from controller.controller_interface import Controller

from resources import config


app = Flask(__name__)

c = Controller()


REST_API = config.REST_API
CRAWLER_START = config.CRAWLER_START
CRAWLER_STOP = config.CRAWLER_STOP
CRAWLER_GET = config.CRAWLER_GET
PARSER_START = config.PARSER_START
PARSER_STOP = config.PARSER_STOP
PARSER_GET = config.PARSER_GET



@app.route(REST_API + CRAWLER_START, methods=['POST'])
def start_crawler(crawler_id):
    res = c.start_crawler(crawler_id)
    return jsonify(res)


@app.route(REST_API + CRAWLER_STOP, methods=['DELETE'])
def stop_crawler(crawler_id):
    res = c.terminate_process(crawler_id)
    if not res:
        abort(404)
    else:
        return jsonify(res)


@app.route(REST_API + CRAWLER_GET, methods=['GET'])
def get_all_crawlers():
    return jsonify(c.get_crawlers())


@app.route(REST_API + PARSER_START, methods=['GET'])
def start_parser():
    res = c.start_parser()
    return jsonify(res)


@app.route(REST_API + PARSER_STOP, methods=['DELETE'])
def stop_parser(parser_id):
    ''' Use UUID sent in response when parsers/start '''
    res = c.terminate_process(parser_id)
    if not res:
        abort(404)
    else:
        return jsonify(res)


@app.route(REST_API + PARSER_GET, methods=['GET'])
def get_all_parsers():
    return jsonify(c.get_parsers)


if __name__ == '__main__':
    app.run(host='192.168.128.232', debug=True)
