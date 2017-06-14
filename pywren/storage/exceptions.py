class StorageNoSuchKeyError(Exception):
    def __init__(self, key):
        msg = "No such key {} found in storage.".format(key)
        super(StorageNoSuchKeyError, self).__init__(msg)


class StorageOutputNotFoundError(Exception):
    def __init__(self, callset_id, call_id):
        msg = "Output for {} {} not found in storage.".format(callset_id, call_id)
        super(StorageNoSuchKeyError, self).__init__(msg)