from flask import Flask, render_template
from flask import request, jsonify
from flask.logging import default_handler
import threading
import time
import logging
import background
import traceback
import os
import requests


app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.logger.propagate = False
app.logger.setLevel(logging.WARN)
wlog = logging.getLogger("werkzeug")
wlog.propagate = False
wlog.setLevel(logging.WARN)

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

def get_progress_list(queue):
    try:
        if queue and queue.progress_messages is not None:
            messagelist = list(queue.progress_messages)
            progress = "\n".join(messagelist)
            progress = f"{progress}"
            return progress
        return ["error getting progress list"]
    except Exception as exc:
        app.logger.warning(f"error getting progress list: {exc}")
        return ["error getting progress list"]
    
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

@app.route("/locs")
def show_locators():
    try:
        locators = background.intent_log.get_records_pd()
        return render_template('locators.html', locators=locators)
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

@app.route("/progress")
def snaptool_progress():
    try:
        progress_local = get_progress_list(background.background_q_local)
        progress_remote = get_progress_list(background.background_q_remote)
        progress_delete = get_progress_list(background.background_q_delete)
        return render_template("progress.html", configobj=sconfig, progress_local=progress_local, progress_remote=progress_remote, progress_delete=progress_delete)
    except Exception as exc:
        html = traceback.format_exc()
        return render_template("error.html", message=f"error: <br><br>{html}")

@app.route("/")
def snaptool_main_menu():
    try:
        app.logger.info(f"snaptool_main_menu rendering...")
        q_size_local = background.background_q_local.qsize()
        q_local = background.background_q_local.queue
        progress_local = get_progress_list(background.background_q_local)
        app.logger.info(f"got progress list local: {progress_local}")
        
        q_size_delete = background.background_q_delete.qsize()
        q_delete = background.background_q_delete.queue
        progress_delete = get_progress_list(background.background_q_delete)
        app.logger.info(f"got progress list delete: {progress_delete}")
        
        q_size_remote = background.background_q_remote.qsize()
        q_remote = background.background_q_remote.queue
        progress_remote = get_progress_list(background.background_q_remote)
        app.logger.info(f"got progress list remote: {progress_remote}")
    except Exception as exc:
        app.logger.error(f"error getting main page background process info: {exc}")
    try:
        if sconfig and sconfig.schedules_dict and sconfig.configfile:
            app.logger.info(f"configobj: {sconfig} ie: {sconfig.ignored_errors} e:{sconfig.errors}")
            app.logger.info(f"scheduleddict: {sconfig.schedules_dict}")
            return render_template("index.html", configobj=sconfig, q_local=q_local, q_size_local=q_size_local, progress_local=progress_local, q_delete=q_delete, q_size_delete=q_size_delete, progress_delete=progress_delete, q_remote=q_remote, q_size_remote=q_size_remote, progress_remote=progress_remote)
        elif not sconfig:
            return render_template("error.html", message="sconfig not initialized")
        elif not sconfig.schedules_dict:
            return render_template("error.html", message="schedules not initialized.  Missing or bad configuration file? ")
        elif not sconfig.configfile:
            return render_template("error.html", message="sconfig configfile not initialized")
    except Exception as exc:
        html = f"{traceback.format_exc()}"
        return render_template("error.html", message=f"error: {html}")

@app.get("/shutdownthread")
def shutdown_thread():
    shutdown_func = request.environ.get('werkzeug.server.shutdown')
    shutdown_func()
    return "Shutting down..."

def stop_ui():
    app.logger.warning(f"Shutting down flask...")
    global sconfig
    try:
        if sconfig:
            url = f'http://127.0.0.1:{sconfig.flask_http_port}/shutdownthread'
            print(f"in stop_ui, url: {url}")
            resp = requests.get(url)
    except Exception as exc:
        app.logger.error(f"While shutting down {exc}")
    app.logger.warning(f"resp = {resp}")

def run_ui(snaptool_config=None):
    global sconfig
    sconfig = snaptool_config
    kwargs = {'host': '0.0.0.0', 'port': sconfig.flask_http_port, 'threaded': True, 'use_reloader': False, 'debug': False}
    flaskThread = threading.Thread(target=app.run, daemon=True, kwargs=kwargs)
    print("run_ui - Starting status UI\n")
    flaskThread.start()
    print("run_ui - UI Thread started\n")

if __name__ == '__main__':
    run_ui(snaptool_config=f" testing - called from flask_ui")
    time.sleep(15)
