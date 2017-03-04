#-*-coding:utf-8-*-
import os
from flask import jsonify
import json

class Response():
    SUCCESS_CODE = 0
    MISSING_PARAMETERS_CODE = -1
    INVALID_PARA_CODE = -2
    CREATE_TASK_FAILED = -3
    DUPLICATED_DAGID_CODE = -4
    DECODE_TASKLIST_FAILED_CODE = -5
    CODE_TO_MSG = {
        SUCCESS_CODE: "success",
        MISSING_PARAMETERS_CODE: "Missing Parameters",
        INVALID_PARA_CODE: "invalid parameters",
        CREATE_TASK_FAILED: "create task failed",
        DUPLICATED_DAGID_CODE: "duplicated dag_id",
        DECODE_TASKLIST_FAILED_CODE: "decode tasklist failed"
    }

    def __init__(self, code, msg=None, data=None):
        self.code = code
        if msg:
            self.msg = msg
        else:
            self.msg = Response.CODE_TO_MSG.get(code, "internel server error")
        if data:
            self._body = {"code": code, "msg": self.msg, "data": data}
        else:
            self._body = {"code": code, "msg": self.msg}

    def __str__(self):
        return json.dumps(self._body)

    def body(self):
        return jsonify(self._body)
