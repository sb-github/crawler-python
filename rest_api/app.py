#!venv/bin/python
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


REST_API = '/api/v1.0/'

app = Flask(__name__)

c = Controller()


@app.route(REST_API + 'crawlers/start/<crawler_id>', methods=['POST'])
def start_crawler(crawler_id):
    res = c.start_crawler(crawler_id)
    return jsonify(res)


@app.route(REST_API + 'crawlers/stop/<crawler_id>', methods=['DELETE'])
def stop_crawler(crawler_id):
    res = c.terminate_process(crawler_id)
    if not res:
        abort(404)
    else:
        return jsonify(res)


@app.route(REST_API + 'crawlers', methods=['GET'])
def get_all_crawlers():
    return jsonify(c.get_crawlers())


@app.route(REST_API + 'parsers/start', methods=['GET'])
def start_parser():
    res = c.start_parser()
    return jsonify(res)


@app.route(REST_API + 'parsers/stop/<parser_id>', methods=['DELETE'])
def stop_parser(parser_id):
    ''' Use UUID sent in response when parsers/start '''
    res = c.terminate_process(parser_id)
    if not res:
        abort(404)
    else:
        return jsonify(res)


@app.route(REST_API + 'parsers', methods=['GET'])
def get_all_parsers():
    return jsonify(c.get_parsers)


if __name__ == '__main__':
    app.run(debug=True)
