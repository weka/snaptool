from flask import Flask, render_template
import threading
import time
import os
import datetime
import background

app = Flask(__name__)
config = None

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

@app.route("/alog")
def show_actions_log():
    try:
        if config.resolved_actions_log is not None:
            with open(config.resolved_actions_log, "r") as f:
                logblob = f.read()
            entries = logblob.splitlines(True)
            revlog = "".join(reversed(entries))
            return render_template('actions_log.html', logtext=revlog)
    except Exception as exc:
        return f"error: {exc}"

@app.route("/config_file")
def show_config_file():
    try:
        if config.configfile is not None:
            with open(config.configfile, "r") as f:
                logblob = f.read()
            return render_template('config_file.html', logtext=logblob)
    except Exception as exc:
        return f"error: {exc}"

@app.route("/config")
def snaptool_config_summary():
    try:
        return render_template("snap-config.html", configobj=config, grouprender=str_schedule_group)
    except Exception as exc:
        return f"error: {exc}"

@app.route("/")
def snaptool_main_menu():
    try:
        q_size = background.background_q.qsize()
        messagelist = list(background.background_q.progress_messages)
        progress = "\n".join(messagelist)
        progress = f"{progress}"
        return render_template("index.html", configobj=config, q_size=q_size, progress=progress)
    except Exception as exc:
        return f"error: {exc}"

def run_ui(snaptool_config=None):
    # app.run(debug=True)
    global config
    config = snaptool_config
    kwargs = {'host': '127.0.0.1', 'port': 5000, 'threaded': True, 'use_reloader': False, 'debug': False}
    flaskThread = threading.Thread(target=app.run, daemon=True, kwargs=kwargs)
    # thread_local_data.snaptool_config = snaptool_config
    flaskThread.start()
    print("run_ui - flaskThread started\n")

if __name__ == '__main__':
    run_ui(snaptool_config=f" testing - called from flask_ui")
    time.sleep(15)
