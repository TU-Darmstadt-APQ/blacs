#####################################################################
#                                                                   #
# /batch_compiler.py                                                #
#                                                                   #
# Copyright 2013, Monash University                                 #
#                                                                   #
# This file is part of the program runmanager, in the labscript     #
# suite (see http://labscriptsuite.org), and is licensed under the  #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################

from labscript_utils.ls_zprocess import ProcessTree
process_tree = ProcessTree.connect_to_parent()
to_parent = process_tree.to_parent
from_parent = process_tree.from_parent
kill_lock = process_tree.kill_lock

# Set a meaningful name for zprocess.locking's client id:
process_tree.zlock_client.set_process_name('blacs.composition_executor')

import os
import sys
import traceback
import shutil
from types import ModuleType
import labscript_utils.h5_lock, h5py

from runmanager import get_runmanager_dir

import labscript
from labscript_utils.modulewatcher import ModuleWatcher

class BatchProcessor(object):
    def __init__(self, to_parent, from_parent, kill_lock):
        self.to_parent = to_parent
        self.from_parent = from_parent
        self.kill_lock = kill_lock
        # Create a module object in which we execute the user's script. From its
        # perspective it will be the __main__ module:
        self.script_module = ModuleType('__main__')
        # Save the dict so we can reset the module to a clean state later:
        self.script_module_clean_dict = self.script_module.__dict__.copy()
        sys.modules[self.script_module.__name__] = self.script_module

        # Start the compiler subprocess:
        self.to_batch_compiler, self.from_batch_compiler, self.batch_compiler = process_tree.subprocess(
            os.path.join(get_runmanager_dir(), 'batch_compiler.py')
        )

        self.mainloop()
        
    def mainloop(self):
        while True:
            signal, data =  self.from_parent.get()
            if signal == 'execute':
                success = self.execute(*data)
                self.to_parent.put(['done',success])
            elif signal == 'quit':
                sys.exit(0)
            else:
                raise ValueError(signal)
                    
    def execute(self, run_file):
        self.script_module.__file__ = run_file

        # Save the current working directory before changing it to the location of the
        # user's script:
        cwd = os.getcwd()
        os.chdir(os.path.dirname(run_file))

        try:
            # Do not let the modulewatcher unload any modules whilst we're working:
            with kill_lock, module_watcher.lock:
                
                labscript.init_run(self.sub_shot_callback, run_file)

                with h5py.File(run_file, 'r') as f:
                    script_code = f['script'].asstr()[()]

                code = compile(
                    script_code, self.script_module.__file__, 'exec', dont_inherit=True
                )
                exec(code, self.script_module.__dict__)

            return True
        except Exception:
            traceback_lines = traceback.format_exception(*sys.exc_info())
            del traceback_lines[1:2]
            message = ''.join(traceback_lines)
            sys.stderr.write(message)
            return False
        finally:    
                        
            labscript.cleanup_run()
            os.chdir(cwd)
            # Reset the script module's namespace:
            self.script_module.__dict__.clear()
            self.script_module.__dict__.update(self.script_module_clean_dict)

    def sub_shot_callback(self, shot_name, shot_id, runfile_path, extra_runglobals = {}):

        # TODO: self.set_status("Preparing sub-shot", composition_filepath=runfile_path)
        with h5py.File(runfile_path,'r+') as runfile:
            
            if runfile['shot_templates'][shot_name] is None:
                raise Exception('ERROR, shot template not found!')

            is_static = runfile['shot_templates'][shot_name].attrs['is_static']

            if is_static:
                shot_filepath = self.prepare_static_sub_shot(runfile, shot_name, shot_id)
            else:
                shot_filepath = self.prepare_dynamic_sub_shot(runfile, shot_name, shot_id, extra_runglobals)

            # Link shot to main hdf5 file
            runfile['shots'].create_group(f'{shot_id:04d}_{shot_name}')
            runfile['shots'][f'{shot_id:04d}_{shot_name}'].attrs['shot_name'] = shot_name
            runfile['shots'][f'{shot_id:04d}_{shot_name}'].attrs['shot_id'] = shot_id
            runfile['shots'][f'{shot_id:04d}_{shot_name}']['data'] = h5py.ExternalLink(shot_filepath, "/")

        self.to_parent.put(['run', [shot_filepath]])

        while True:
            signal, data = self.from_parent.get()
            if signal == 'finish_run':
                success = data
                if not success:
                    raise Exception(f'Could not run shot "{shot_filepath}"')
                break
            else:
                raise RuntimeError((signal, data))

        return shot_filepath

    def prepare_dynamic_sub_shot(self, runfile, shot_name, shot_id, extra_runglobals = {}):

        # find location of shot template
        shot_template_link = runfile['shot_templates'][shot_name]
        shot_folder = runfile.attrs['sub_shot_runs_folder']

        shot_template_filepath = shot_template_link.file.filename
        shot_filepath = f"{shot_folder}/{shot_id:04d}_{shot_name}.h5"

        # Create shot file from template
        shutil.copy(shot_template_filepath, shot_filepath)

        with h5py.File(shot_filepath,'r+') as f:
            labscript_path = f.attrs['dynamic_script']

            for name, value in extra_runglobals.items():
                if value is None:
                    # Store it as a null object reference:
                    value = h5py.Reference()
                try:
                    f['globals'].attrs[name] = value
                except Exception as e:
                    message = ('Global %s cannot be saved as an hdf5 attribute. ' % name +
                            'Globals can only have relatively simple datatypes, with no nested structures. ' +
                            'Original error was:\n' +
                            '%s: %s' % (e.__class__.__name__, str(e)))
                    raise ValueError(message)

        # TODO: self.set_status("Compiling sub-shot...", composition_filepath=runfile.filename)
        # compile sub-shot
        self.to_batch_compiler.put(['compile', [labscript_path, shot_filepath]])
        while True:
            signal, data = self.from_batch_compiler.get()
            if signal == 'done':
                success = data
                if not success:
                    raise Exception('Could not dynamically compile sub-shot')
                break
            else:
                raise RuntimeError((signal, data))

        return shot_filepath

    def prepare_static_sub_shot(self, runfile, shot_name, shot_id):

        # find location of shot template
        shot_template_link = runfile['shot_templates'][shot_name]
        shot_folder = runfile.attrs['sub_shot_runs_folder']

        shot_template_filepath = shot_template_link.file.filename
        shot_filepath = f"{shot_folder}/{shot_id:04d}_{shot_name}.h5"

        # Create shot file from template
        shutil.copy(shot_template_filepath, shot_filepath)

        return shot_filepath
                   
if __name__ == '__main__':
    module_watcher = ModuleWatcher() # Make sure modified modules are reloaded
    # Rename this module to '_runmanager_batch_compiler' and put it in sys.modules under
    # that name. The user's script will become the __main__ module:
    __name__ = '_blacs_composition_executor'
    sys.modules[__name__] = sys.modules['__main__']
    batch_processor = BatchProcessor(to_parent,from_parent,kill_lock)
