#
# Copyright 2018 PyWren Team
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
from __future__ import absolute_import
from __future__ import print_function

import sys
import types

if sys.version < '3':
    try:
        from cStringIO import StringIO
    except ImportError:
        from io import BytesIO as StringIO
    PY3 = False
else:
    types.ClassType = type
    from io import BytesIO as StringIO
    PY3 = True

# pylint: disable=wrong-import-position
from pywren.serialize.cloudpickle import CloudPickler
from pywren.serialize.module_dependency import ModuleDependencyAnalyzer
from pywren.serialize import default_preinstalls

class SerializeIndependent(object):
    def __init__(self, preinstalled_modules=default_preinstalls.modules):
        # pylint: disable=dangerous-default-value
        self.preinstalled_modules = preinstalled_modules
        self._modulemgr = None

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
    serialize = SerializeIndependent()

    def foo(x):
        y = x + 10
        return y + 1

    sb, paths = serialize(foo, 6)
    print("paths=", paths)
