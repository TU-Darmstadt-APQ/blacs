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
import h5py
from qtutils import UiLoader
from qtutils import inmain_decorator
from qtutils.qt.QtCore import QTimer
from blacs.plugins import PLUGINS_DIR
import logging

name = "Progress Bar APQ"
module = "progress_bar_apq"  # should be folder name
logger = logging.getLogger('BLACS.plugin.%s' % module)


class Plugin(object):
    def __init__(self, initial_settings):
        self.menu = None
        self.notifications = {}
        self.BLACS = None
        self.ui = None
        self.shots = {}
        self.initial_max = 0
        self.shot_model = None

    def get_menu_class(self):
        return None

    def get_notification_classes(self):
        return []

    def get_setting_classes(self):
        return []

    def get_callbacks(self):
        return {'shot_complete': self.on_shot_complete}

    def set_menu_instance(self, menu):
        self.menu = menu

    def set_notification_instances(self, notifications):
        self.notifications = notifications

    def plugin_setup_complete(self, BLACS):
        self.BLACS = BLACS
        self.shot_model = self.BLACS['experiment_queue']._model

        self.ui = UiLoader().load(os.path.join(PLUGINS_DIR, module, 'progressbar.ui'))
        # Add our controls to the BLACS gui:
        self.BLACS['ui'].queue_status_verticalLayout.insertWidget(1, self.ui)

        rowcount = self.shot_model.rowCount()
        if rowcount > 0:
            self.on_new_shots(None, 0, rowcount - 1)

        self.shot_model.rowsInserted.connect(self.on_new_shots)
        self.shot_model.rowsAboutToBeRemoved.connect(self.on_removed_shots)
        self.shot_model.modelReset.connect(self.on_removed_all_shots)

    def on_shot_complete(self, h5_filepath):
        try:
            with h5py.File(h5_filepath) as h5_file:
                n_runs = int(h5_file.attrs['n_runs'])
                run_number = int(h5_file.attrs['run number'])
        except Exception:
            return

        self.update_shots_left()
        self.update_progress(run_number + 1, n_runs)

    def on_new_shots(self, parent, start, end):
        for i in range(start, end + 1):
            h5_filepath = self.shot_model.item(i).text()
            try:
                with h5py.File(h5_filepath) as h5_file:
                    master_pseudoclock = h5_file['connection table'].attrs['master_pseudoclock']
                    stoptime = h5_file['devices/%s' % master_pseudoclock].attrs['stop_time']
            except Exception:
                stoptime = 0
            self.shots[self.shot_model.item(i).text()] = stoptime

        self.update_shots_left()

    def on_removed_shots(self, parent, start, end):
        for i in range(start, end + 1):
            h5_filepath = self.shot_model.item(i).text()
            if h5_filepath in self.shots.keys():
                del self.shots[h5_filepath]

        self.update_shots_left()

    def on_removed_all_shots(self):
        self.shots = {}
        self.update_shots_left()
        QTimer.singleShot(200, lambda: self.update_progress(0, 0))

    @inmain_decorator(False)
    def update_shots_left(self):
        shots = self.shots
        n_shots = len(shots)
        total_seconds = sum(list(shots.values())) + n_shots * 0.200
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        self.ui.shotsLeftLabel.setText("{} ({:.0f} hrs {:.0f} min {:.0f} secs)".format(n_shots, hours, minutes, seconds))

    @inmain_decorator(False)
    def update_progress(self, value, maximum):
        self.ui.currentProgressBar.setValue(value)
        self.ui.currentProgressBar.setMaximum(maximum)

    def get_save_data(self):
        return {}

    def close(self):
        pass
