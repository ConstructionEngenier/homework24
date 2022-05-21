import os
import re
from typing import Iterator, Optional

from flask import Flask, request, Response
from werkzeug.exceptions import BadRequest

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def run_command(iter_obj: Iterator, query: str) -> Iterator:
    query_items = query.split('|')
    data = iter(map(lambda v: v.strip(), iter_obj))
    for item in query_items:
        if ":" in item:
            cmd_value = item.split(':')
            cmd = cmd_value[0]
            value = cmd_value[1]
        else:
            cmd = item
            value = ""
        data = get_data(data, cmd, value)
    return data


def get_data(iter_obj: Iterator, cmd: str, value: str) -> Iterator:
    if cmd == "filter":
        return filter(lambda v: value in v, iter_obj)
    if cmd == "map":
        return map(lambda v: v.split(" ")[int(value)], iter_obj)
    if cmd == "unique":
        return iter(set(iter_obj))
    if cmd == "sort":
        if value == "desc":
            return iter(sorted(iter_obj, reverse=True))
        else:
            return iter(sorted(iter_obj))
    if cmd == "limit":
        return get_limit(iter_obj, int(value))
    if cmd == "regex":
        regex = re.compile(value)
        return filter(lambda v: regex.search(v), iter_obj)
    return iter_obj


def get_limit(iter_obj: Iterator, value: int) -> Iterator:
    i = 0
    for item in iter_obj:
        if i < value:
            yield item
        else:
            break
        i += 1


@app.post("/perform_query")
def perform_query() -> Response:
    try:
        query = request.args['query']
        file_name = request.args["file_name"]
    except KeyError:
        raise BadRequest

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        raise BadRequest(description=f"{file_name} was not found")

    with open(file_path) as iter_obj:
        result = run_command(iter_obj, query)
        content = '\n'.join(result)
        # print(content)

    return app.response_class(content, content_type="text/plain")
