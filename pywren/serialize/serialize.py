from __future__ import absolute_import

import sys
import types

try:
    from six.moves import cPickle as pickle
except:
    import pickle

try:
    from pywren.cStringIO import StringIO
except:
    if sys.version < '3':
        from pickle import Pickler
        try:
            from cStringIO import StringIO
        except ImportError:
            from io import BytesIO
        PY3 = False
    else:
        types.ClassType = type
        from pickle import _Pickler as Pickler
        from io import BytesIO as StringIO
        PY3 = True

import numpy as np

from pywren.serialize.cloudpickle import CloudPickler
from pywren.serialize.module_dependency import ModuleDependencyAnalyzer
from pywren.serialize import default_preinstalls

# class Serialize(object):
#     def __init__(self):

#         pass

#     def __call__(self, f, *args, **kwargs):
#         """
#         Serialize
#         """

#         self._modulemgr = ModuleDependencyAnalyzer()
#         preinstalled_modules = [name for name, _ in preinstalls.modules]
#         self._modulemgr.ignore(preinstalled_modules)

#         f_kwargs = {}
#         for k, v in kwargs.items():
#             if not k.startswith('_'):
#                 f_kwargs[k] = v
#                 del kwargs[k]

#         s = StringIO()
#         cp = CloudPickler(s, 2)
#         cp.dump((f, args, f_kwargs))

#         if '_ignore_module_dependencies' in kwargs:
#             ignore_modulemgr = kwargs['_ignore_module_dependencies']
#             del kwargs['_ignore_module_dependencies']
#         else:
#             ignore_modulemgr = False

#         if not ignore_modulemgr:
#             # Add modules
#             for module in cp.modules:
#                 print "adding module", module, module.__name__
#                 self._modulemgr.add(module.__name__)

#             print 'inspected modules', self._modulemgr._inspected_modules
#             print 'modules to inspect', self._modulemgr._modules_to_inspect
#             print 'paths to trans', self._modulemgr._paths_to_transmit

#             mod_paths = self._modulemgr.get_and_clear_paths()
#             print "mod_paths=", mod_paths



#         return cp, s.getvalue(), mod_paths
        #     vol_name = self._get_auto_module_volume_name()
        # if self._modulemgr.has_module_dependencies:
        #         v = self.multyvac.volume.get(vol_name)
        #         if not v:
        #             try:
        #                 self.multyvac.volume.create(vol_name, '/pymodules')
        #             except RequestError as e:
        #                 if 'name already exists' not in e.message:
        #                     raise
        #             v = self.multyvac.volume.get(vol_name)
        #         if mod_paths:
        #             v.sync_up(mod_paths, '')

        # kwargs['_stdin'] = s.getvalue()
        # kwargs['_result_source'] = 'file:/tmp/.result'
        # kwargs['_result_type'] = 'pickle'
        # if not ignore_modulemgr and self._modulemgr.has_module_dependencies:
        #     kwargs.setdefault('_vol', []).append(vol_name)
        # # Add to the PYTHONPATH if user is using it as well
        # env = kwargs.setdefault('_env', {})
        # if env.get('PYTHONPATH'):
        #     env['PYTHONPATH'] = env['PYTHONPATH'] + ':/pymodules'
        # else:
        #     env['PYTHONPATH'] = '/pymodules'

        # tags = kwargs.setdefault('_tags', {})
        # # Make sure function name fits within length limit for tags
        # fname = JobModule._func_name(f)
        # if len(fname) > 100:
        #     fname = fname[:97] + '...'
        # tags['fname'] = fname

        # return self.shell_submit(
        #     'python -m multyvacinit.pybootstrap',
        #     **kwargs
        # )

class SerializeIndependent(object):
    def __init__(self, preinstalled_modules=default_preinstalls.modules):

        self.preinstalled_modules = preinstalled_modules

    def __call__(self, list_of_objs, **kwargs):
        """
        Serialize f, args, kwargs independently
        """

        self._modulemgr = ModuleDependencyAnalyzer()
        preinstalled_modules = [name for name, _ in self.preinstalled_modules]
        self._modulemgr.ignore(preinstalled_modules)

        # f_kwargs = {}
        # for k, v in kwargs.items():
        #     if not k.startswith('_'):
        #         f_kwargs[k] = v
        #         del kwargs[k]

        cps = []
        strs = []
        for obj in list_of_objs:
            s = StringIO()
            cp = CloudPickler(s, 2)
            cp.dump(obj)
            cps.append(cp)
            strs.append(s)

        if '_ignore_module_dependencies' in kwargs:
            ignore_modulemgr = kwargs['_ignore_module_dependencies']
            del kwargs['_ignore_module_dependencies']
        else:
            ignore_modulemgr = False

        if not ignore_modulemgr:
            # Add modules
            for cp in cps:
                for module in cp.modules:
                    self._modulemgr.add(module.__name__)
            # FIXME add logging
            #print 'inspected modules', self._modulemgr._inspected_modules
            #print 'modules to inspect', self._modulemgr._modules_to_inspect
            #print 'paths to trans', self._modulemgr._paths_to_transmit

            mod_paths = self._modulemgr.get_and_clear_paths()
            #print "mod_paths=", mod_paths

        return ([s.getvalue() for s in strs], mod_paths)

if __name__ == "__main__":
    #logging.basicConfig(level=logging.DEBUG)
    #mda = ModuleDependencyAnalyzer()


    import testmod

    serialize = SerializeIndependent()

    def foo(x):
        y = testmod.bar_square(x) + np.arange(3)
        return y + 1

    cp, sb, paths = serialize(foo, 7)
    print(cp.modules)
    print("paths=", paths)
