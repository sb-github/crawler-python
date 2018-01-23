#!venv/bin/python
from flask import Flask, jsonify, abort
from .controller_interface import Controller


REST_API = '/api/v1.0/'

app = Flask(__name__)

c = Controller()


@app.route(REST_API + 'crawlers/<int:crawler_id>', methods=['GET'])
def get_crawler(crawler_id):
    if not c.get_crawler_by_id(crawler_id):
        abort(404)
    else:
        return jsonify(c.get_crawler_by_id(crawler_id))


@app.route(REST_API + 'crawlers/create/<int:crawler_id>', methods=['POST'])
def create_crawler(crawler_id):
    res = c.start_crawler(crawler_id)
    return jsonify(res)


@app.route(REST_API + 'crawlers/<int:crawler_id>/delete', methods=['DELETE'])
def delete_crawler(crawler_id):
    res = c.stop_crawler(crawler_id)
    if not res:
        abort(404)
    else:
        # return jsonify(c.stop_crawler(crawler_id))
        return jsonify(res)


@app.route(REST_API + 'crawlers', methods=['GET'])
def get_all_crawlers():
    return jsonify(c.get_crawlers())


@app.route(REST_API + 'parsers/<int:parser_id>', methods=['GET'])
def get_parser(parser_id):
    pass


@app.route(REST_API + 'parsers/create', methods=['POST'])
def create_parser():
    pass


@app.route(REST_API + 'parsers/<int:parser_id>/delete', methods=['DELETE'])
def delete_parser():
    pass



if __name__ == '__main__':
    app.run(debug=True)
