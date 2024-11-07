# import flask
import functions_framework
import os
import vertexai
from vertexai.generative_models import GenerativeModel

PROJECT_ID = "wev-dev-analytics"
LOCATION = "asia-northeast3"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "wev-dev-analytics-30e1a0019654.json"
vertexai.init(project=PROJECT_ID, location=LOCATION)

model = GenerativeModel("gemini-1.5-flash-002")

@functions_framework.http
def gcr_test(request: flask.Request) -> flask.typing.ResponseReturnValue:

    if request.path == '/print':
        return print_handler()
    elif request.path == '/post_test':
        return post_test(request)
    else:
        return flask.Response("Here for TEST!", 200)

@functions_framework.http
def print_handler():
    return flask.Response("Hello My Friend!", 200)

@functions_framework.http
def post_test(request: flask.Request):
    query = request.get_json().get("query")
    res = model.generate_content(query)
    return flask.Response(res.to_dict()['candidates'][0]['content']['parts'][0]['text'], 200)