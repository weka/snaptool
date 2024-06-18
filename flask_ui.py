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
import yamale
import yaml
from datetime import datetime



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

def get_logs():
    try:
        if background.background_q and background.background_q.progress_messages is not None:
            messagelist = list(background.background_q.progress_messages)
            progress = "\n".join(messagelist)
            progress = f"{progress}"
            return progress
        return ["error getting progress list"]
    except Exception as exc:
        app.logger.warning(f"error getting progress list: {exc}")
        return ["error getting progress list"]
    
@app.route("/locs")
def show_locators():
    try:
        locators = background.intent_log.get_records_pd()
        return render_template('locators.html', locators=locators)
    except Exception as exc:
        html = traceback.format_exc()
        return render_template("error.html", message=f"error: <br><br>{html}")

@app.route("/all_snaps")
def show_all_snaps():
    try:
        allsnaps = sconfig.cluster_connection.get_snapshots()
        return render_template('all_snaps.html', allsnaps=allsnaps)
    except Exception as exc:
        html = traceback.format_exc()
        return render_template("error.html", message=f"error: <br><br>{html}")

@app.route("/config_file")
def show_config_file():
    try:
        if request.method == "GET":
            if sconfig.configfile is not None:
                if os.path.exists(sconfig.configfile):
                    with open(sconfig.configfile, "r") as f:
                        contents = f.read()
                    if sconfig.args.no_edit is False:
                        return render_template('config_file_edit.html', filetext=contents, msgtext="")
                    else:
                        return render_template('config_file_view.html', filetext=contents, msgtext="")
                else:
                    return render_template("error.html", message=f"{sconfig.configfile} not found")
    except Exception as exc:
        html = traceback.format_exc()
        return render_template("error.html", message=f"error: {html}")

@app.route("/config_file_submit", methods=["POST"])
def config_file_submit():
    try:
        if sconfig.args.no_edit is True:
            return render_template("error.html", 
                                   message=f"error: config_file_submit, but editing disabled.")
        if request.method == "POST":
            changedtxt = request.form.get('configtextinput', default="Oops - get returned nothing")
            try:
                schema = yamale.make_schema("./static/snaptool-config-schema.yaml")
                data = yamale.make_data(content=changedtxt)
                validate_list = yamale.validate(schema, data)
                with open(sconfig.configfile, "w") as f:
                    f.write(changedtxt)
                msgs = f"Saved.  No first-pass syntax errors found.\nFile is {sconfig.configfile}."
                msgs += f"\n\nAny changes should be picked up by Snaptool within a minute."
                return render_template('config_file_edit.html', 
                                   filetext=f"{changedtxt}", 
                                   msgtext=msgs)
            except ValueError as exc:
                html = traceback.format_exc()
                msgs = [str(v) for v in exc.results]
                return render_template('config_file_edit.html', 
                    filetext=f"{changedtxt}", 
                    msgtext=f"Not Saved! Error validating yaml: \n{exc}")
            except yaml.parser.ParserError as exc:
                html = traceback.format_exc()
                return render_template('config_file_edit.html', 
                    filetext=f"{changedtxt}", 
                    msgtext=f"Not Saved! Parse error in yaml (indent or space/tab error?): \n\n{exc}")
    except Exception as exc:
        html = traceback.format_exc()
        return render_template("error.html", message=f"error: {html}")

@app.route("/log")
def show_logs():
    try:
        logs = get_logs()
        return render_template("log.html", configobj=sconfig, logs=logs)
    except Exception as exc:
        html = traceback.format_exc()
        return render_template("error.html", message=f"error: <br><br>{html}")

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

@app.route("/")
def snaptool_main_menu():
    try:
        app.logger.info(f"snaptool_main_menu rendering...")
        q_size = background.background_q.qsize()
        q = background.background_q.queue
        progress = get_logs()
        app.logger.info(f"got progress list: {progress}")
    except Exception as exc:
        app.logger.error(f"error getting main page background process info: {exc}")
    try:
        if sconfig and sconfig.schedules_dict and sconfig.configfile:
            app.logger.info(f"configobj: {sconfig} ie: {sconfig.ignored_errors} e:{sconfig.errors}")
            app.logger.info(f"scheduleddict: {sconfig.schedules_dict}")
            servertime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return render_template("index.html", configobj=sconfig, q=q, q_size=q_size, 
                                   servertime=servertime,
                                   progress=progress)
        elif not sconfig:
            return render_template("error.html", message="sconfig not initialized")
        elif not sconfig.schedules_dict:
            return render_template("error.html", message="schedules not initialized.  Missing or bad configuration file? ")
        elif not sconfig.configfile:
            return render_template("error.html", message="sconfig configfile not initialized")
    except Exception as exc:
        html = f"{traceback.format_exc()}"
        return render_template("error.html", f"error: {html}")

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
