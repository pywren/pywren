class StorageNoSuchKeyError(Exception):
    def __init__(self, key):
        msg = "No such key {} found in storage.".format(key)
        super(StorageNoSuchKeyError, self).__init__(msg)

class StorageOutputNotFoundError(Exception):
    def __init__(self, callset_id, call_id):
        msg = "Output for {} {} not found in storage.".format(callset_id, call_id)
        super(StorageOutputNotFoundError, self).__init__(msg)

class StorageConfigMismatchError(Exception):
    def __init__(self, current_path, prev_path):
        msg = "The data is stored at {}, but current storage is configured at {}.".format(
            prev_path, current_path)
        super(StorageConfigMismatchError, self).__init__(msg)

