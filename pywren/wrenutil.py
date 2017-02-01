import uuid
import os
import shutil
import glob
import wrenhandler

def uuid_str():
    return str(uuid.uuid4())


SOURCE_DIR = os.path.dirname(os.path.abspath(__file__)) 

class WrappedStreamingBody:
    """
    Wrap boto3's StreamingBody object to provide enough Python fileobj functionality 
    so that tar/gz can happen in memory
    
    from https://gist.github.com/debedb/2e5cbeb54e43f031eaf0

    """
    def __init__(self, sb, size):
        # The StreamingBody we're wrapping
        self.sb = sb
        # Initial position
        
        self.pos = 0
        # Size of the object
        
        self.size = size

    def tell(self):
        #print("In tell()")
        
        return self.pos

    def readline(self):
        #print("Calling readline()")
        
        try:
            retval = self.sb.readline()
        except struct.error:
            raise EOFError()
        self.pos += len(retval)
        return retval


    def read(self, n=None):
        retval = self.sb.read(n)
        if retval == "":
            raise EOFError()
        self.pos += len(retval)
        return retval

    def seek(self, offset, whence=0):
        #print("Calling seek()")                          
        retval = self.pos
        if whence == 2:
            if offset == 0:
                retval = self.size
            else:
                raise Exception("Unsupported")
        else:
            if whence == 1:
                offset = self.pos + offset
                if offset > self.size:
                    retval = self.size
                else:
                    retval = offset
        # print("In seek(%s, %s): %s, size is %s" % (offset, whence, retval, self.size))
        
        self.pos = retval
        return retval

    def __str__(self):
        return "WrappedBody"

    def __getattr__(self, attr):
        # print("Calling %s"  % attr)
        
        if attr == 'tell':
            return self.tell
	elif attr == 'seek':
            return self.seek
        elif attr == 'read':
            return self.read
        elif attr == 'readline':
            return self.readline
        elif attr == '__str__':
            return self.__str__
        else:
            return getattr(self.sb, attr)



def sdb_to_dict(item):
    attr = item['Attributes']
    return {c['Name'] : c['Value'] for c in attr }

def local_handler(jobs, run_dir, extra_context = None):
    """
    Run a list of (deserialized) jobs locally inside of 
    run_dir

    Just for debugging
    """

    def copy_runtime(tgt_dir):
        files = glob.glob(os.path.join(SOURCE_DIR, "./*.py"))
        for f in files:
            shutil.copy(f, os.path.join(tgt_dir, os.path.basename(f)))

    original_dir = os.getcwd()
    jobn = len(jobs)

    for job_i, job in enumerate(jobs):
        task_run_dir = os.path.join(run_dir, str(job_i))
        shutil.rmtree(task_run_dir, True) # delete old modules
        os.makedirs(task_run_dir)
        copy_runtime(task_run_dir)


        context = {'jobnum' : job_i}
        if extra_context is not None:
            context.update(extra_context)

        os.chdir(task_run_dir)
        wrenhandler.generic_handler(job, context)

        os.chdir(original_dir)
    
