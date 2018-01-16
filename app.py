#!venv/bin/python
from flask import Flask, jsonify, abort
from .controller_interface import Controller


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
    return jsonify(c.status())


@app.route(REST_API + 'parsers/start/<parser_id>', methods=['POST'])
def start_parser(parser_id):
    pass


@app.route(REST_API + 'parsers/stop/<parser_id>', methods=['DELETE'])
def stop_parser():
    pass


if __name__ == '__main__':
    app.run(debug=True)
