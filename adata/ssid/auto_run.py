import subprocess
import os
import sys

homePath = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))


def runCommand(command):
    """
    """
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print output.strip()
    returncode = process.wait()
    return returncode


def run_local(mode, load_date):
    """
    """
    sparkHome = ["/Users/lzh/spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit"]
    master = ["--master", "local[*]"]
    scriptPath = ["%s/bin/daily_update.py" % homePath]
    pyPackage = ["--py-files", "%s/lib/leek.zip" % homePath]
    pyConf = ["--conf", "spark.finogeeks.sbs.conf.path=%s" % homePath]
    command = sparkHome + master + pyPackage + pyConf + scriptPath + [mode, load_date]
    runCommand(command)


def run_cluster(mode, load_date):
    """
    """
    sparkHome = ["spark2-submit"]
    master = ["--master", "yarn"]
    resource = ["--num-executors", "8", "--executor-cores", "2", "--executor-memory", "4g"]
    scriptPath = ["%s/bin/daily_update.py" % homePath]
    pyConf = ["--conf", "spark.finogeeks.sbs.conf.path=%s" % homePath]
    pyPackage = ["--py-files", "%s/lib/leek.zip" % homePath]
    command = sparkHome + master + resource + pyPackage + pyConf + scriptPath + [mode, load_date]
    runCommand(command)


if __name__ == "__main__":
    if len(sys.argv) == 4:
        _, env, mode, load_date = sys.argv
        if env == "local":
            run_local(mode, load_date)
        else:
            run_cluster(mode, load_date)
    else:
        print >> sys.stderr, """
        Usage: auto_run.py <env> <mode> <load_date>
        env contains local or cluster
        mode contains init or update:
        summary means building gfuser; detail means building gfuser_detail
        Load_date is the format of 2015-11-11
        """
