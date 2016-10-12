# Generic notes

# Notes on setting up a new user
1. first we need to create the iam role and the associated policy 
2. Package up runner and create lambda function
3. Make sure best practices are followed for 

# Tests from travis:
1. create the IAM role and policy
2. create and deploy the function
3. invoke the function
4. delete the role, job running detritus, etc. 



Attach full policy


create_config
test_config
create_role()
deploy_lambda
delete_lambda

encrypting variables
https://docs.travis-ci.com/user/environment-variables#Defining-encrypted-variables-in-.travis.yml

# what type of instances
from the example getting `/proc/cpuinfo`

```

processor       : 1
vendor_id       : GenuineIntel
cpu family      : 6
model           : 62
model name      : Intel(R) Xeon(R) CPU E5-2680 v2 @ 2.80GHz
stepping        : 4
microcode       : 0x428
cpu MHz         : 2800.076
cache size      : 25600 KB
physical id     : 0
siblings        : 2
core id         : 0
cpu cores       : 1
apicid          : 1
initial apicid  : 1
fpu             : yes
fpu_exception   : yes
cpuid level     : 13
wp              : yes
flags           : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx rdtscp lm constant_tsc rep_good nopl xtopology eagerfpu pni pclmulqdq ssse3 cx16 pcid sse4_1 sse4_2 x2apic popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm fsgsbase smep erms xsaveopt
bugs            :
bogomips        : 5600.15
clflush size    : 64
cache_alignment : 64
address sizes   : 46 bits physical, 48 bits virtual
power management:

```

The existance of AVX is a good sign
