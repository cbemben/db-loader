import pickle

class FileMonitor:
  """Returns list of files that are new in a directory or target files that have changed.
     This relies on a pickle file that stores metadata about the directory or file that is
     used to compare against the current state of the directory/file.
     
     Note: All files must live in a single directory
  """
  def __init__(self, data_dir: str, target_file: str):
      self._data_dir = data_dir
      self._target_file = target_file
      self._pickle_filename = 'updater-scope.pickle'
  
  def get_dir_path(self):
      """This is the filepath to the target file to be monitored"""
      return self._data_dir.joinpath(self._target_file)
    
  def get_dir_contents(self):
      """This returns a list of all files in the target directory"""
      return list(self._data_dir.iterdir())
    
  def get_target_metadata(self):
      return self.get_dir_path().stat()
    
  def get_modified_diff(self):
      curr_mod_time = self.get_target_metadata().st_mtime
      prev = self.get_scope_from_disc()
      prev_mod_time = prev.get(self._target_file, 0)
      return curr_mod_time > prev_mod_time
    
  def write_scope_to_disc(self, scope) -> None:
      with open(self._pickle_filename, 'w+b') as wf:
        pickle.dump(scope, wf, pickle.HIGHEST_PROTOCOL)
    
  def get_scope_to_disc(self) -> dict:
      try:
        with open(self._pickle_filename, 'rb') as rf:
          return pickle.load(rf)
        except (OSError, IOError) as e:
          data = {self._target_file: self.get_target_metadata().st_mtime}
          self.write_scope_to_disc(data)
          return data
  
  def get_file_list(self):
      file_path = None
      if self.get_modified_diff():
        prev = self.get_scope_from_disc()
        prev[self._target_file] = self.get_target_metadata().st_mtime
        self.write_scope_to_disc(prev)
        file_path = self.get_dir_path()
      return file_path
    
if __name__ == '__main__':
  dirz = 'path/to/directory/with/target/file'
  file = 'filename.csv'
  f = FileMonitor(data_dir=dirz, target_file=file)
