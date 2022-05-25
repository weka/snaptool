from flask import Flask, render_template
from flask.logging import default_handler
from flask import request
import threading
import time
import logging
import background
import traceback
import os

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.logger.propagate = False
app.logger.setLevel(logging.WARN)
wlog = logging.getLogger("werkzeug")
wlog.propagate = False
wlog.setLevel(logging.WARN)
print(f"flask - root logger: {logging.getLogger()}")
print(f"flask - app logger: {app.logger}")
print(f"flask - logger: {wlog}")

sconfig = None

def str_schedule(schedule):
    html = f"<br>&emsp; {schedule.get_html()}"
    return html

def str_schedule_group(group):
    html = f"<p>{group.name}:"
    fs = None
    if len(group.filesystems) > 0:
        fs = ", ".join(group.filesystems)
    html += f"<br>&emsp; filesystems: {fs}"        
    for sg in group.entries:
        html += str_schedule(sg)
    return html

def get_progress_list():
    try:
        if background.background_q:
            if background.background_q.progress_messages:
                messagelist = list(background.background_q.progress_messages)
                progress = "\n".join(messagelist)
                progress = f"{progress}"
                return progress
    except Exception as exc:
        app.logger.warning(f"error getting progress list: {exc}")
        return "error getting progress list"
    
@app.route("/alog")
def show_actions_log():
    try:
        if sconfig and sconfig.resolved_actions_log and sconfig.resolved_actions_log is not None:
            with open(sconfig.resolved_actions_log, "r") as f:
                logblob = f.read()
            entries = logblob.splitlines(True)
            revlog = "".join(reversed(entries))
            return render_template('actions_log.html', logtext=revlog)
    except Exception as exc:
        html = traceback.format_exc()
        return render_template("error.html", message=f"error: <br><br>{html}")

@app.route("/config_file")
def show_config_file():
    try:
        if sconfig.configfile is not None:
            if os.path.exists(sconfig.configfile):
                with open(sconfig.configfile, "r") as f:
                    contents = f.read()
                return render_template('config_file.html', filetext=contents)
            else:
                return render_template("error.html", message=f"{sconfig.configfile} not found")
    except Exception as exc:
        html = traceback.format_exc()
        return render_template("error.html", message=f"error: {html}")

@app.route("/config")
def snaptool_config_summary():
    try:
        progress = get_progress_list()
        return render_template("snap-config.html", configobj=sconfig, progress=progress)
    except Exception as exc:
        html = traceback.format_exc()
        return render_template("error.html", message=f"error: <br><br>{html}")

@app.route("/")
def snaptool_main_menu():
    try:
        app.logger.info(f"snaptool_main_menu rendering...")
        q_size = background.background_q.qsize()
        q = background.background_q.queue
        progress = get_progress_list()
        app.logger.info(f"got progress list: {progress}")
    except Exception as exc:
        app.logger.error(f"error getting main page background process info: {exc}")
    try:
        if sconfig and sconfig.schedules_dict and sconfig.configfile:
            app.logger.info(f"configobj: {sconfig} ie: {sconfig.ignored_errors} e:{sconfig.errors}")
            app.logger.info(f"scheduleddict: {sconfig.schedules_dict}")
            return render_template("index.html", configobj=sconfig, q=q, q_size=q_size, progress=progress)
        elif not sconfig:
            return render_template("error.html", message="sconfig not initialized")
        elif not sconfig.schedules_dict:
            return render_template("error.html", message="schedules not initialized.  Missing or bad configuration file? ")
        elif not sconfig.configfile:
            return render_template("error.html", message="sconfig configfile not initialized")
    except Exception as exc:
        html = f"{traceback.format_exc()}"
        return render_template("error.html", f"error: {html}")

def run_ui(snaptool_config=None):
    # app.run(debug=True)
    global sconfig
    sconfig = snaptool_config
    kwargs = {'host': '0.0.0.0', 'port': sconfig.flask_http_port, 'threaded': True, 'use_reloader': False, 'debug': True}
    flaskThread = threading.Thread(target=app.run, daemon=True, kwargs=kwargs)
    print("run_ui - Starting status UI\n")
    flaskThread.start()
    print("run_ui - UI Thread started\n")

if __name__ == '__main__':
    run_ui(snaptool_config=f" testing - called from flask_ui")
    time.sleep(15)
