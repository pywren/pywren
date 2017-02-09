# Notes about the conda runtime

## Where is the space going? 

How large are the pyc files:
` find . -name "*.pyc" | xargs ls -l | awk '{x+=$5} END {print "total bytes: " x}' `

~42MB

How large are the so files:
` find . -name "*.so" | xargs ls -l | awk '{x+=$5} END {print "total bytes: " x}' `


~338 MB 

what are the big files and directories? 

` du -a . | sort -n -r | head -n 40 `
 
 
 ## Stripping symbols from shared libraries
 
 Are there debug symbols?
 http://stackoverflow.com/questions/1999654/how-can-i-tell-if-a-library-was-compiled-with-g
 
 objdump --debugging libvoidincr.a
 
 (lots of crap)
 
 Following instructions here:
 
 https://www.technovelty.org/linux/stripping-shared-libraries.html
 
 
 -rwxrwxr-x 1 ec2-user ec2-user 7.4M Oct  4 15:25 lib/python2.7/site-packages/numpy/core/multiarray.so
[ec2-user@ip-172-31-47-207 condaruntime]$ strip --strip-all lib/python2.7/site-packages/numpy/core/multiarray.so
[ec2-user@ip-172-31-47-207 condaruntime]$ ls -lah lib/python2.7/site-packages/numpy/core/multiarray.so
-rwxrwxr-x 1 ec2-user ec2-user 1.7M Jan  4 20:38 lib/python2.7/site-packages/numpy/core/multiarray.so

On the MKL runtime we get:
[ec2-user@ip-172-31-47-207 condaruntime]$ strip --strip-all lib/libmkl_avx2.so
BFD: lib/storZe24: Not enough room for program headers, try linking with -N
strip:lib/storZe24[.note.gnu.build-id]: Bad value
BFD: lib/storZe24: Not enough room for program headers, try linking with -N
strip:lib/storZe24: Bad value

[ec2-user@ip-172-31-47-207 condaruntime]$ ls -lah lib/libpython2.7.so.1.0
-rwxr-xr-x 1 ec2-user ec2-user 6.4M Jul  2  2016 lib/libpython2.7.so.1.0
[ec2-user@ip-172-31-47-207 condaruntime]$ strip --strip-all lib/libpython2.7.so.1.0
[ec2-user@ip-172-31-47-207 condaruntime]$ ls -lah lib/libpython2.7.so.1.0
-rwxr-xr-x 1 ec2-user ec2-user 1.9M Jan  4 20:39 lib/libpython2.7.so.1.0


