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
from qtutils import inmain_decorator, UiLoader
from blacs.plugins import PLUGINS_DIR

class Plugin(object):
    def __init__(self, initial_settings):
        self.menu = None
        self.notifications = {}
        self.BLACS = None
        self.filter = True
        self.shot_model = None
        self.queue_repeat = False
        self.active = False
        self.active = initial_settings.get('active', False)

    def get_menu_class(self):
        return None

    def get_notification_classes(self):
        return []

    def get_setting_classes(self):
        return []

    def get_callbacks(self):
        return {'analysis_cancel_send': self.analysis_filter, 'shot_complete': self.shot_complete}

    def set_menu_instance(self, menu):
        self.menu = menu

    def set_notification_instances(self, notifications):
        self.notifications = notifications

    def plugin_setup_complete(self, BLACS):
        self.BLACS = BLACS
        self.ui = UiLoader().load(os.path.join(PLUGINS_DIR, 'repeat_first_shot', 'controls.ui'))
        self.shot_model = self.BLACS['experiment_queue']._model
        self.BLACS['ui'].queue_pause_button.toggled.connect(self.queue_paused)
        self.ui.checkBox_repeateFirst.toggled.connect(self.update_active)
        self.BLACS['ui'].queue_repeat_button.toggled.connect(self.update_queue_repeat)
        self.queue_repeat = self.BLACS['ui'].queue_repeat_button.isChecked()
        self.BLACS['ui'].queue_controls_frame.layout().addWidget(self.ui)
        self.ui.checkBox_repeateFirst.setChecked(self.active)

    def analysis_filter(self, h5_filepath):
        if self.filter:
            self.filter = False
            if not self.queue_repeat and self.active:
                self.BLACS['experiment_queue'].clean_h5_file(h5_filepath, 'temp.h5')
                os.remove(h5_filepath)
                os.rename('temp.h5', h5_filepath)
                self.BLACS['experiment_queue'].prepend(h5_filepath)
                return True
        return False

    @inmain_decorator(wait_for_return=True)
    def shot_complete(self, h5_filepath):
        if self.shot_model.rowCount == 0:
            self.filter = True

    def queue_paused(self, checked):
        if checked:
            self.filter = True

    def update_queue_repeat(self, checked):
        self.queue_repeat = checked

    def update_active(self, checked):
        self.active = checked

    def get_save_data(self):
        return {'active': self.ui.checkBox_repeateFirst.isChecked()}

    def close(self):
        pass
