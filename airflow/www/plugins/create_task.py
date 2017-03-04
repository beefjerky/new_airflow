#-*-coding:utf-8-*-
# This is the class you derive to create a plugin
import os
import logging
import airflow
import airflow.api
from flask_admin import BaseView, expose
from flask_admin.base import MenuLink
from flask import (
    g, Markup, Blueprint, redirect, jsonify, abort, request, current_app, send_file
)
# Importing base classes that we need to derive
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator, DagModel
from airflow.plugins_manager import AirflowPlugin
from airflow.executors.base_executor import BaseExecutor
from airflow import settings
from airflow.www.app import csrf
from airflow import configuration as conf
from jdb_common import Response
import json
import traceback


# Will show up under airflow.hooks.PluginHook
class PluginHook(BaseHook):
    pass

# Will show up under airflow.operators.PluginOperator
class PluginOperator(BaseOperator):
    pass

# Will show up under airflow.executors.PluginExecutor
class PluginExecutor(BaseExecutor):
    pass

# Creating a flask admin BaseView
class CreateTask(BaseView):
    @expose('/')
    def create(self):
        # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.html
        #return self.render("test_plugin/test.html", content="Hello galaxy!")
        return "in create test page"

    def is_visible(self):
        return True

requires_authentication = None

v = CreateTask(category="Plugins", name="create task")

# Creating a flask blueprint to intergrate the templates and static folder
bp = Blueprint(
    "jdbapi", __name__,
    template_folder='templates', # registers airflow/plugins/templates as a Jinja template folder
    static_folder='static')

dag_file_header = """
#-*-coding:utf-8-*-
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import timedelta

"""

@csrf.exempt
@bp.route("/jdb/api/create_task", methods=['POST'])
def create_task():
    dagid = request.args.get("dagid")
    sch_interval = request.args.get("sch_interval")
    email = request.args.get("email")
    tasks = request.args.get("tasklist")
    if not dagid or not sch_interval or not email or not tasks:
        return Response(Response.MISSING_PARAMETERS_CODE).body()
    session = settings.Session()
    dag = session.query(DagModel).filter(
        DagModel.dag_id == dagid).first()
    session.commit()
    if dag:
        return Response(Response.DUPLICATED_DAGID_CODE).body()


    task_list = []

    try:
        task_list = json.loads(tasks)
    except:
        logging.error("error in json load task list", traceback.format_exc())
        return Response(Response.DECODE_TASKLIST_FAILED_CODE).body()
    dag_path = conf.get("core", "dags_folder")
    tmp_file = "/tmp/" + dagid + ".py"
    fd = open(tmp_file, "w")
    fd.write(dag_file_header)

    create_dag = """
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id='%s', default_args=args,
    schedule_interval='%s')
    """ % (dagid, sch_interval)
    fd.write(create_dag)
    for i, task in enumerate(task_list):
        shell_cmd = '\\nexec %s\\n' % task['path']
        create_task_str = '\n%s = BashOperator(task_id="%s", bash_command="%s", dag=dag)\n' \
                          % (task['taskid'], task['taskid'], shell_cmd)
        fd.write(create_task_str)
    for task in task_list:
        for dep in task['dep']:
            dep_str = "%s.set_upstream(%s)\n" % (task['taskid'], dep)
            fd.write(dep_str)
    fd.close()
    ret = os.system("cd /tmp/;python %s.py" % dagid)
    if ret != 0:
        return Response(Response.CREATE_TASK_FAILED).body()
    os.system("cp %s %s" % (tmp_file, dag_path))
    ret = os.system("airflow list_tasks %s" % dagid)
    if ret != 0:
        try:
            os.remove(dag_path + "/" + dagid + ".py")
        except:
            pass
        return Response(Response.CREATE_TASK_FAILED).body()
    return Response(Response.SUCCESS_CODE).body()

# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "plugins"
    operators = [PluginOperator]
    flask_blueprints = [bp]
    hooks = [PluginHook]
    executors = [PluginExecutor]
    admin_views = [v]
