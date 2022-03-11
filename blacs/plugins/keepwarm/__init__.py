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

import os
import labscript_utils.h5_lock
import platform
import h5py
import shutil
from qtutils import inmain_decorator, UiLoader
from labscript_utils.labconfig import LabConfig
from qtutils.qt import QtWidgets
from qtutils.qt.QtCore import Qt
from labscript_utils.qtwidgets.elide_label import elide_label
from blacs.plugins import PLUGINS_DIR


class Plugin(object):
    def __init__(self, initial_settings):
        self.menu = None
        self.notifications = {}
        self.BLACS = None
        self.shot_model = None
        self.active = initial_settings.get('active', True)
        self.keep_warm_file = None
        self.watchdog_triggered = False

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
        self.ui = UiLoader().load(os.path.join(PLUGINS_DIR, 'keepwarm', 'controls.ui'))
        self.shot_model = self.BLACS['experiment_queue']._model
        self.BLACS['ui'].queue_controls_frame.layout().addWidget(self.ui)
        self.ui.checkBox_keepwarm.setChecked(self.active)
        self.append_keep_warm(check_repeat=False)
        self.ui.checkBox_keepwarm.toggled.connect(self.keepwarm_toogled)
        self.BLACS['ui'].queue_pause_button.toggled.connect(self.append_keep_warm)
        self.BLACS['ui'].queue_abort_button.clicked.connect(self.append_keep_warm)

    def keepwarm_toogled(self, checked):
        self.active = checked
        if checked:
            self.append_keep_warm(check_repeat=False)

    def analysis_filter(self, h5_filepath):
        self.keep_warm_file = self.BLACS['settings'].get_value(Setting, 'keep_warm_file')
        if self.keep_warm_file is not None and h5_filepath == self.keep_warm_file:
            return True
        return False

    def repeat_filter(self, h5_filepath):
        self.keep_warm_file = self.BLACS['settings'].get_value(Setting, 'keep_warm_file')
        if self.keep_warm_file is not None and h5_filepath == self.keep_warm_file:
            return True
        return False

    def shot_complete(self, h5_filepath):
        self.keep_warm_file = self.BLACS['settings'].get_value(Setting, 'keep_warm_file')
        self.append_keep_warm(check_repeat=(self.keep_warm_file != h5_filepath))

    @inmain_decorator(True)
    def append_keep_warm(self, check_repeat=True):
        self.keep_warm_file = self.BLACS['settings'].get_value(Setting, 'keep_warm_file')
        if self.active and self.keep_warm_file is not None and os.path.isfile(self.keep_warm_file):
            if not self.BLACS['experiment_queue'].manager_paused:
                repeat = self.BLACS['experiment_queue'].manager_repeat
                watchdog_triggered = self.watchdog_triggered  # remove race condition
                if ((self.shot_model.rowCount() == 0) & (not (repeat & check_repeat))) or watchdog_triggered:
                    self.BLACS['experiment_queue'].clean_h5_file(self.keep_warm_file, 'temp.h5')
                    shutil.move('temp.h5', self.keep_warm_file)
                    if not watchdog_triggered:
                        self.BLACS['experiment_queue'].append([self.keep_warm_file])
                    else:
                        self.BLACS['experiment_queue'].prepend(self.keep_warm_file)

    def get_save_data(self):
        return {'active': self.active}

    def watchdog_triggered_keepwarm(self, state):
        self.watchdog_triggered = state

    def close(self):
        pass


class Setting(object):
    name = "Keep Warm"

    def __init__(self, data):
        # This is our data store!
        self.data = data
        required_config_params = {"paths": ["experiment_shot_storage"]}
        self.exp_config = LabConfig(required_params=required_config_params)

        if 'keep_warm_file' not in self.data:
            self.data['keep_warm_file'] = None

    # Create the GTK page, return the page and an icon to use on the label (the class name attribute will be used for the label text)
    def create_dialog(self, notebook):
        self.ui = UiLoader().load(os.path.join(PLUGINS_DIR, 'keepwarm', 'settings.ui'))

        # get the widgets!
        self.widgets = {}
        self.widgets['label'] = self.ui.file_label
        self.widgets['label'].setText(self.data['keep_warm_file'])

        self.ui.file_btn.clicked.connect(self.selectKeepWarmFile)
        return self.ui, None

    def selectKeepWarmFile(self):
        keep_warm_file = QtWidgets.QFileDialog.getOpenFileName(self.ui,
                                                               'Select a keep warm script',
                                                               self.exp_config.get('paths', 'experiment_shot_storage'),
                                                               "H5 File (*.h5)")
        if type(keep_warm_file) is tuple:
            keep_warm_file, _ = keep_warm_file
        self.data['keep_warm_file'] = keep_warm_file
        self.widgets['label'].setText(keep_warm_file)

    def get_value(self, name):
        if name in self.data:
            return self.data[name]

        return None

    def save(self):
        # transfer the contents of the list store into the data store, and then return the data store
        return self.data

    def close(self):
        pass
