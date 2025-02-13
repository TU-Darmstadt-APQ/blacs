#####################################################################
#                                                                   #
# /plugins/general/__init__.py                                      #
#                                                                   #
# Copyright 2013, Monash University                                 #
#                                                                   #
# This file is part of the program BLACS, in the labscript suite    #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################

from blacs.tab_base_classes import PluginTab
from qtutils.qt.QtWidgets import *
from qtutils.qt.QtCore import *
from qtutils.qt.QtGui import *
from labscript_utils.qtwidgets.toolpalette import ToolPaletteGroup
from qtutils import UiLoader
import os
import threading
import shutil
from zprocess import TimeoutError
from labscript_utils.ls_zprocess import zmq_get
import time
from qtutils import inmain_decorator
import ast
import logging
from logging.handlers import RotatingFileHandler
from blacs.plugins import PLUGINS_DIR, callback

debug = True


class Plugin(object):
    def __init__(self, initial_settings):
        self.logger = logging.getLogger("Ueberwachungs Log")
        self.logger.setLevel(logging.INFO)
        if debug:
            self.handler = RotatingFileHandler("ExpUe.log", maxBytes=1024 * 1024 * 50, backupCount=1)
            self.logger.addHandler(self.handler)
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
            self.handler.setFormatter(formatter)

        self.menu = None
        self.notifications = {}
        self.BLACS = None
        self.tab = None
        self.close = False
        self.shot_file = None
        self.pause_triggered = False
        self.error_count = 0

    def get_menu_class(self):
        return None

    def get_notification_classes(self):
        return []

    def get_setting_classes(self):
        return [Setting]

    def get_callbacks(self):
        return {'analysis_cancel_send': self.analysis_filter, 'shot_complete': self.shot_complete, 'shot_ignore_repeat': self.repeat_filter}

    def set_menu_instance(self, menu):
        self.menu = menu

    def set_notification_instances(self, notifications):
        self.notifications = notifications

    def plugin_setup_complete(self, BLACS):
        self.BLACS = BLACS

        self.serverlist = self.BLACS['settings'].get_value(Setting, 'server_list')
        self.mainloop_thread = threading.Thread(target=self.mainloop)
        self.mainloop_thread.daemon = True
        self.mainloop_thread.start()

    def mainloop(self):
        while not self.close:
            try:
                locks = {}
                # Update Locks
                self.logger.info('Querying Servers')
                for server, port in self.serverlist:
                    try:
                        new_locks = zmq_get(port, server, data="get_locks", timeout=0.2)
                    except TimeoutError:
                        pass
                    except Exception as e:
                        self.logger.exception(str(e))
                    else:
                        if isinstance(new_locks, dict):
                            self.logger.info('Updating Locks with: {}'.format(new_locks))
                            locks.update(new_locks)

                    if len(locks) > 0:
                        self.logger.info('Updating Widgets: {}'.format(locks))
                        self.update_widgets(locks)
                        self.logger.info('Done updating Widgets')
            except Exception as e:
                self.logger.exception(str(e))
            time.sleep(1)

    def trigger_reset(self):
        for server, port in self.serverlist:
            try:
                zmq_get(port, server, data="set_locked", timeout=0.2)
            except Exception:
                pass

    def analysis_filter(self, h5_filepath):
        return self.pause_triggered

        # This callback should run after other callbacks (There is a cycle-time plugin as PR planned which probably should realy run last ->priority=100)
    @callback(priority=15)
    def shot_complete(self, h5_filepath):
        if self.pause_triggered:
            self.BLACS['experiment_queue'].clean_h5_file(h5_filepath, 'temp_watchdog.h5')
            shutil.move('temp_watchdog.h5', h5_filepath)
            if 'keepwarm' in self.BLACS['plugins'] and self.BLACS['plugins']['keepwarm'].active:
                # The keep warm file must always be prepended even if watchdog is active:
                if h5_filepath == self.BLACS['plugins']['keepwarm'].keep_warm_file:
                    self.BLACS['experiment_queue'].prepend(h5_filepath)
                else:
                    # Repeated shot is added at the second position of the queue if keep_warm is active
                    # as the keep warm file must be prepended and the repeated file should
                    # go second.
                    self.BLACS['experiment_queue'].prepend_second_position(h5_filepath)
            else:
                self.BLACS['experiment_queue'].prepend(h5_filepath)
            self.error_count += 1
            # refresh the locks after the shot to retry. We already keep track of the error count.
            self.trigger_reset()
        else:
            self.error_count = 0
        self.tab.update_failed_locks(self.error_count)

    def repeat_filter(self, h5_filepath):
        return self.pause_triggered

    @inmain_decorator(True)
    def update_widgets(self, locks):
        # Pause Queue if any if the lock items is checked and not locked
        if any([self.tab.controlWidget.update_item(name, lock) for name, lock in locks.items()]):
            self.pause_triggered = True
            if 'keepwarm' in self.BLACS['plugins'] and self.BLACS['plugins']['keepwarm'].active:
                self.BLACS['plugins']['keepwarm'].watchdog_triggered_keepwarm(True)
            else:
                self.BLACS['experiment_queue'].manager_paused = True
        else:
            self.pause_triggered = False
            if 'keepwarm' in self.BLACS['plugins'] and self.BLACS['plugins']['keepwarm'].active:
                self.BLACS['plugins']['keepwarm'].watchdog_triggered_keepwarm(False)

    def get_tab_classes(self):
        return {'BigBrother': TestTab}

    @inmain_decorator(True)
    def tabs_created(self, tabs_dict):
        self.tab = tabs_dict['BigBrother']
        tabs_dict['BigBrother'].plugin = self

    def get_save_data(self):
        return {}

    def close(self):
        self.close = True
        self.mainloop_thread.join()


class TestTab(PluginTab):
    def initialise_GUI(self):
        self.layout = self.get_tab_layout()

        resetbtn = QPushButton("Reset Locks")
        resetbtn.clicked.connect(self.reset_clicked)
        self.layout.addWidget(resetbtn)

        self.plugin = None
        self.state = 'idle'
        self.shutdown_workers_complete = False

        self.controlWidget = ControlWidget()
        self.layout.addWidget(self.controlWidget)
        self.layout.addItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.MinimumExpanding))

        self.label = QLabel("Failed locks in a row: 0")
        self.layout.addWidget(self.label)

    def update_failed_locks(self, num):
        self.label.setText(f"Failed locks in a row: {num}")

    def reset_clicked(self):
        if self.plugin is not None:
            self.plugin.trigger_reset()

    def get_save_data(self):
        return {}

    def restore_save_data(self, data):
        return

    def shutdown_workers(self):
        self.shutdown_workers_complete = True
        return

    def finalise_close_tab(self, currentpage):
        return


class ControlWidget(QWidget):
    """A Widget providing a Grid for ControlItems and keeps track of them"""

    def __init__(self):
        super(ControlWidget, self).__init__()
        tpg = ToolPaletteGroup(self)
        self.flowbox = tpg.append_new_palette('Locks')
        self.items = {}

    def add_item(self, name):
        """Adds a new ControlItem to the Grid with the provided name"""
        item = ControlItem(name, parent=self)
        self.flowbox.addWidget(item)
        self.items[name] = item

    def remove_item(self, name):
        """Removes ControlItem by name"""
        item = self.items.pop(name)
        item.hide()
        self.flowbox.removeWidget(item)

    @inmain_decorator(True)
    def update_item(self, name, locked):
        """Updates a ControlItem's locked state by name"""
        if name not in self.items:
            self.add_item(name)

        self.items[name].set_lock(locked)

        return (not locked and self.items[name].isChecked())


class ControlItem(QCheckBox):
    """A Q Widget concisting of a Button to display the locked state and a checkbox
       that lets the user choose to pause experiments in Blacs"""

    def __init__(self, name, parent=None):
        super(QCheckBox, self).__init__(name, parent)
        self.setCheckable(True)
        self.setEnabled(True)
        self.setChecked(True)
        self.lock = True

    def set_lock(self, lock):
        self.lock = lock
        if lock:
            self.setStyleSheet("QCheckBox{ background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1, stop: 0 #48dd48, stop: 1 #20ff20);}")
        else:
            self.setStyleSheet("QCheckBox{ background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1, stop: 0 #B41414, stop: 1 #DC0000);}")


class Setting(object):
    name = "BigBrother"

    def __init__(self, data):
        # This is our data store!
        self.data = data

        if 'server_list' not in self.data:
            self.data['server_list'] = []

    # Create the GTK page, return the page and an icon to use on the label (the class name attribute will be used for the label text)
    def create_dialog(self, notebook):
        ui = UiLoader().load(os.path.join(PLUGINS_DIR, 'ueberwachung', 'servers.ui'))

        # get the widgets!
        self.widgets = {}
        self.widgets['server_list'] = ui.server_list
        self.widgets['server_list'].setText(str(self.data['server_list']))

        return ui, None

    def get_value(self, name):
        if name in self.data:
            return self.data[name]

        return None

    def save(self):
        # transfer the contents of the list store into the data store, and then return the data store
        try:
            self.data['server_list'] = ast.literal_eval(self.widgets['server_list'].toPlainText())
        except Exception:
            raise Exception("Server/Port specification probably not correct. Write >['192.168.1.xxx',31642]<")
        return self.data

    def close(self):
        pass
