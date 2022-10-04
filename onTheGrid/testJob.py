import htcondor
#
# The stub of the HTCondor job to submit to the PPD batch system
#
sub = htcondor.Submit()

sub["Executable"] = "test.sh"
sub["Universe"] = "vanilla"
sub["output"] = "output/log-$(Cluster)-$(Process)-$(SUBMIT_TIME).out" 
sub["error"] = "output/err-$(Cluster)-$(Process)-$(SUBMIT_TIME).out" 
sub["request_memory"] = "2 GB" 
sub["requirements"] = """(Opsys =?= "LINUX") && (OpSysAndVer =?= "CentOS7")""" 

schedd = htcondor.Schedd()
